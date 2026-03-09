"""
IDF helpers for the EnergyPlus parametric study workflow.

Original parametric functions: Daniele Borraccino <borraccino1@phd.poliba.it>
Pipeline integration & modifications: Sanjay Somanath <sanjay.somanath@chalmers.se>

Division of Sustainable Built Environments, Chalmers University of Technology
In collaboration with Politecnico di Bari

License: MIT
"""

import numpy as np
import pandas as pd
from eppy.modeleditor import IDF
from scipy.stats import qmc


def build_zone_mapping(idf_path, output_path):
    """Build per-zone metadata from the baseline IDF and save it as CSV."""
    temp_idf = IDF(idf_path)
    hvac_zones = {
        thermostat.Zone_or_ZoneList_Name
        for thermostat in temp_idf.idfobjects["ZONECONTROL:THERMOSTAT"]
    }
    floor_surfaces_by_zone = {}
    for surface in temp_idf.idfobjects["BUILDINGSURFACE:DETAILED"]:
        if surface.Surface_Type.lower() == "floor":
            floor_surfaces_by_zone.setdefault(surface.Zone_Name, []).append(surface)

    mapping_data = []
    for zone in temp_idf.idfobjects["ZONE"]:
        name = zone.Name
        is_heated = name in hvac_zones
        surfaces = floor_surfaces_by_zone.get(name, [])
        floor_area = 0
        z_coords = []
        for surface in surfaces:
            floor_area += surface.area
            z_coords.append(sum(vertex[2] for vertex in surface.coords) / len(surface.coords))
        avg_z = sum(z_coords) / len(z_coords) if z_coords else 0
        if floor_area == 0:
            floor_area = 20.0
        mapping_data.append({
            "Zone_Name": name,
            "Is_Heated": is_heated,
            "Z_Level": round(avg_z, 2),
            "Area_sqm": round(floor_area, 2),
        })

    mapping_df = pd.DataFrame(mapping_data)
    if not mapping_df[mapping_df["Is_Heated"]].empty:
        heated_df = mapping_df[mapping_df["Is_Heated"]]
        min_z = heated_df["Z_Level"].min()
        max_z = heated_df["Z_Level"].max()

        def classify(row):
            if not row["Is_Heated"]:
                return "EXCLUDED"
            if row["Z_Level"] == min_z:
                return "GROUND"
            if row["Z_Level"] == max_z:
                return "TOP"
            return "MIDDLE"

        mapping_df["Cluster"] = mapping_df.apply(classify, axis=1)
    else:
        mapping_df["Cluster"] = "EXCLUDED"

    mapping_df.to_csv(output_path, index=False)
    return mapping_df


def generate_lhs_samples(n_samples, seed=42):
    """Generate the Latin Hypercube sample matrix for the study."""
    ranges = {
        "f_wall": (1.0, 1.5, 0.01),
        "f_roof": (1.0, 1.5, 0.01),
        "f_win": (1.0, 1.3, 0.01),
        "setpoint": (17.0, 24.0, 0.1),
        "hours": (2, 14, 1),
        "inf": (0.3, 1.5, 0.05),
        "vent": (0.0, 0.8, 0.05),
        "misc": (1.0, 6.0, 0.2),
        "shgc": (0.1, 0.9, 0.05),
        "s_hours": (1, 16, 1),
        "cop": (0.65, 0.95, 0.05),
    }
    sampler = qmc.LatinHypercube(d=len(ranges), seed=seed)
    sample_raw = sampler.random(n_samples)
    mins = np.array([value[0] for value in ranges.values()])
    maxs = np.array([value[1] if value[1] > value[0] else value[1] + 1e-7 for value in ranges.values()])
    sample_scaled = qmc.scale(sample_raw, mins, maxs)

    lhs_rows = []
    for row in sample_scaled:
        rounded_row = {}
        for index, (name, value) in enumerate(ranges.items()):
            step = value[2]
            rounded_row[name] = round(row[index] / step) * step
        lhs_rows.append(rounded_row)
    return pd.DataFrame(lhs_rows).round(3)


def _translate_hours_to_strings(hours):
    """Convert the heating-hours selector into schedule transition times."""
    hours = int(hours)
    early_morning, evening_stop = 10, 22
    if hours <= 4:
        return "08:00", "08:00", f"{evening_stop - hours:02d}:00", f"{evening_stop}:00"
    if hours <= 8:
        return f"{early_morning - (hours - 4):02d}:00", f"{early_morning}:00", "18:00", "22:00"
    return "06:00", "10:00", f"{evening_stop - (hours - 4):02d}:00", f"{evening_stop}:00"


def _translate_shading(hours_duration):
    """Calculate opening and closing times symmetrical to 14:00."""
    half_duration = float(hours_duration) / 2
    start_decimal = 14.0 - half_duration
    end_decimal = 14.0 + half_duration

    def to_hm(decimal_hour):
        h = int(decimal_hour)
        m = int((decimal_hour - h) * 60)
        return f"{h:02d}:{m:02d}"

    return to_hm(start_decimal), to_hm(end_decimal)


def apply_parametric_inputs_to_idf(idf_obj, mapping_df, f_wall, f_roof, f_win,
                                   v_set, v_hours, v_inf, v_vent, v_misc,
                                   v_cop, v_shgc, v_s_hours):
    """Apply one study sample to an IDF object."""
    target_zones = set(
        mapping_df[mapping_df["Cluster"] != "EXCLUDED"]["Zone_Name"].astype(str)
    )
    zone_by_name = {zone.Name: zone for zone in idf_obj.idfobjects["ZONE"]}
    surface_by_name = {
        surface.Name: surface
        for surface in idf_obj.idfobjects["BUILDINGSURFACE:DETAILED"]
    }
    construction_by_name = {
        construction.Name: construction
        for construction in idf_obj.idfobjects["CONSTRUCTION"]
    }
    material_by_name = {
        material.Name: material for material in idf_obj.idfobjects["MATERIAL"]
    }
    simple_glazing_by_name = {
        material.Name: material
        for material in idf_obj.idfobjects["WINDOWMATERIAL:SIMPLEGLAZINGSYSTEM"]
    }
    glazing_by_name = {
        material.Name: material
        for material in idf_obj.idfobjects["WINDOWMATERIAL:GLAZING"]
    }
    modified_materials = set()

    for surface in idf_obj.idfobjects["BUILDINGSURFACE:DETAILED"]:
        if surface.Outside_Boundary_Condition.lower() != "outdoors":
            continue
        surface_type = surface.Surface_Type.lower()
        factor = f_wall if surface_type == "wall" else f_roof if surface_type in ["roof", "ceiling"] else 1.0
        construction = construction_by_name.get(surface.Construction_Name)
        if construction is None:
            continue
        layers = [construction.Outside_Layer]
        layers.extend(
            getattr(construction, f"Layer_{index}", None)
            for index in range(2, 11)
            if getattr(construction, f"Layer_{index}", None)
        )
        for mat_name in [layer for layer in layers[:-1] if layer is not None]:
            if mat_name in modified_materials:
                continue
            material = material_by_name.get(mat_name)
            if material and material.Conductivity > 0.05:
                material.Conductivity = round(material.Conductivity * factor, 4)
                material.Density = round(material.Density * (1 + (factor - 1) * 0.5), 1)
                modified_materials.add(mat_name)

    for surface in idf_obj.idfobjects["FENESTRATIONSURFACE:DETAILED"]:
        if surface.Surface_Type.lower() != "window":
            continue
        parent = surface_by_name.get(surface.Building_Surface_Name)
        if not parent or parent.Zone_Name not in target_zones:
            continue
        construction = construction_by_name.get(surface.Construction_Name)
        if construction is None:
            continue
        material_name = construction.Outside_Layer
        if material_name in modified_materials:
            continue
        material = simple_glazing_by_name.get(material_name) or glazing_by_name.get(material_name)
        if material:
            if hasattr(material, "Conductivity"):
                material.Conductivity = round(material.Conductivity * f_win, 4)
            if hasattr(material, "U_Factor"):
                material.U_Factor = round(material.U_Factor * f_win, 3)
            modified_materials.add(material_name)

    shading_controls = idf_obj.idfobjects["WINDOWSHADINGCONTROL"]
    shading_schedule_name = "Master_Shading_Availability_S"
    for existing in [s for s in idf_obj.idfobjects["SCHEDULE:COMPACT"] if s.Name == shading_schedule_name]:
        idf_obj.removeidfobject(existing)
    shading_open, shading_close = _translate_shading(v_s_hours)
    
    shading_schedule = idf_obj.newidfobject(
        "SCHEDULE:COMPACT",
        Name=shading_schedule_name,
        Schedule_Type_Limits_Name="On/Off",
    )

    shading_fields = [
        "Through: 31 Dec",
        "For: AllDays",
        f"Until: {shading_open}", "0.0",
        f"Until: {shading_close}", "1.0",
        "Until: 24:00", "0.0"
    ]
                                       
    for index, value in enumerate(shading_fields):
        shading_schedule[f"Field_{index + 1}"] = value

    for control in shading_controls:
        control.Schedule_Name = shading_schedule_name
        control.Shading_Control_Type = "OnIfScheduleAllows"

    if shading_controls:
        mat_name = shading_controls[0].Shading_Device_Material_Name
        shading_material = idf_obj.getobject("WINDOWMATERIAL:SHADE", mat_name)
        if shading_material:
            shading_material.Solar_Transmittance = round(v_shgc, 3)
            shading_material.Solar_Reflectance = round(1.0 - v_shgc - 0.1, 3)

    morning_start, morning_end, evening_start, evening_end = _translate_hours_to_strings(v_hours)
    setpoint_schedule_name = "Master_Heating_Setpoint_S"
    availability_schedule_name = "Master_Heating_Availability_S"
    for schedule_name in [setpoint_schedule_name, availability_schedule_name]:
        for existing_schedule in [
            schedule
            for schedule in idf_obj.idfobjects["SCHEDULE:COMPACT"]
            if schedule.Name == schedule_name
        ]:
            idf_obj.removeidfobject(existing_schedule)

    setpoint_schedule = idf_obj.newidfobject(
        "SCHEDULE:COMPACT",
        Name=setpoint_schedule_name,
        Schedule_Type_Limits_Name="Temperature",
    )
    setpoint_fields = [
        "Through: 31 Mar", "For: AllDays",
        f"Until: {morning_start}", "12.0", f"Until: {morning_end}", str(v_set),
        f"Until: {evening_start}", "12.0", f"Until: {evening_end}", str(v_set), "Until: 24:00", "12.0",
        "Through: 30 Nov", "For: AllDays", "Until: 24:00", "12.0",
        "Through: 31 Dec", "For: AllDays",
        f"Until: {morning_start}", "12.0", f"Until: {morning_end}", str(v_set),
        f"Until: {evening_start}", "12.0", f"Until: {evening_end}", str(v_set), "Until: 24:00", "12.0",
    ]
    for index, value in enumerate(setpoint_fields):
        setpoint_schedule[f"Field_{index + 1}"] = value

    availability_schedule = idf_obj.newidfobject(
        "SCHEDULE:COMPACT",
        Name=availability_schedule_name,
        Schedule_Type_Limits_Name="Fraction",
    )
    availability_fields = [
        "Through: 31 Mar", "For: AllDays",
        f"Until: {morning_start}", "0.0", f"Until: {morning_end}", "1.0",
        f"Until: {evening_start}", "0.0", f"Until: {evening_end}", "1.0", "Until: 24:00", "0.0",
        "Through: 30 Nov", "For: AllDays", "Until: 24:00", "0.0",
        "Through: 31 Dec", "For: AllDays",
        f"Until: {morning_start}", "0.0", f"Until: {morning_end}", "1.0",
        f"Until: {evening_start}", "0.0", f"Until: {evening_end}", "1.0", "Until: 24:00", "0.0",
    ]
    for index, value in enumerate(availability_fields):
        availability_schedule[f"Field_{index + 1}"] = value

    for dual_setpoint in idf_obj.idfobjects["THERMOSTATSETPOINT:DUALSETPOINT"]:
        dual_setpoint.Heating_Setpoint_Temperature_Schedule_Name = setpoint_schedule_name
    for hvac in idf_obj.idfobjects["ZONEHVAC:IDEALLOADSAIRSYSTEM"]:
        hvac.Heating_Availability_Schedule_Name = availability_schedule_name

    for item in idf_obj.idfobjects["ZONEINFILTRATION:DESIGNFLOWRATE"]:
        zone = zone_by_name.get(item.Zone_or_ZoneList_Name)
        if zone:
            item.Design_Flow_Rate_Calculation_Method = "Flow/Zone"
            item.Design_Flow_Rate = (v_inf * float(zone.Volume)) / 3600.0
            item.Air_Changes_per_Hour = ""

    for item in idf_obj.idfobjects["ZONEVENTILATION:DESIGNFLOWRATE"]:
        zone = zone_by_name.get(item.Zone_or_ZoneList_Name)
        if zone:
            item.Design_Flow_Rate_Calculation_Method = "Flow/Zone"
            item.Design_Flow_Rate = (v_vent * float(zone.Volume)) / 3600.0
            item.Air_Changes_per_Hour = ""

    for equipment in idf_obj.idfobjects["OTHEREQUIPMENT"]:
        equipment.Design_Level_Calculation_Method = "Watts/Area"
        equipment.Power_per_Zone_Floor_Area = v_misc
        equipment.Design_Level = ""


def configure_heating_output_variables(idf_obj):
    """Keep only the hourly heating-energy output variable."""
    for output_variable in list(idf_obj.idfobjects["OUTPUT:VARIABLE"]):
        idf_obj.removeidfobject(output_variable)
    idf_obj.newidfobject(
        "OUTPUT:VARIABLE",
        Key_Value="*",
        Variable_Name="Zone Ideal Loads Supply Air Total Heating Energy",
        Reporting_Frequency="Hourly",
    )
