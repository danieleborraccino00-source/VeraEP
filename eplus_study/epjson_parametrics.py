"""epJSON helpers for the EnergyPlus parametric study workflow."""

import json
import os
import shutil
import subprocess

def ensure_baseline_epjson(idf_path, epjson_path, converter_path):
    """Create or refresh the cached baseline epJSON file."""
    if os.path.exists(epjson_path) and os.path.getmtime(epjson_path) >= os.path.getmtime(idf_path):
        return epjson_path

    output_dir = os.path.dirname(epjson_path) or "."
    os.makedirs(output_dir, exist_ok=True)

    converted_name = f"{os.path.splitext(os.path.basename(idf_path))[0]}.epJSON"
    converted_path = os.path.join(output_dir, converted_name)

    proc = subprocess.run(
        [converter_path, "-f", "epjson", "-o", output_dir, idf_path],
        capture_output=True,
        check=False,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "ConvertInputFormat failed")
    if not os.path.exists(converted_path):
        raise FileNotFoundError(f"Converted epJSON not found at {converted_path}")

    if os.path.abspath(converted_path) != os.path.abspath(epjson_path):
        if os.path.exists(epjson_path):
            os.remove(epjson_path)
        shutil.move(converted_path, epjson_path)

    return epjson_path


def load_epjson_text(epjson_path):
    """Load cached baseline epJSON text for fast per-simulation cloning."""
    with open(epjson_path, "r", encoding="utf-8") as handle:
        return handle.read()


def write_epjson(model, output_path):
    """Write a compact epJSON file for EnergyPlus input."""
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(model, handle, separators=(",", ":"))


def _schedule_fields(values):
    return [{"field": value} for value in values]


def _translate_hours_to_strings(hours):
    """Convert the heating-hours selector into schedule transition times."""
    hours = int(hours)
    early_morning, evening_stop = 10, 22
    if hours <= 4:
        return "08:00", "08:00", f"{evening_stop - hours:02d}:00", f"{evening_stop}:00"
    if hours <= 8:
        return f"{early_morning - (hours - 4):02d}:00", f"{early_morning}:00", "18:00", "22:00"
    return "06:00", "10:00", f"{evening_stop - (hours - 4):02d}:00", f"{evening_stop}:00"


def _translate_shading(hours_open, hour_close):
    hour_close = int(hour_close)
    hour_open = hour_close - int(hours_open)
    return f"{hour_open:02d}:00", f"{hour_close:02d}:00"



def apply_parametric_inputs_to_epjson(model, mapping_df, f_wall, f_roof, f_win,
                                      v_set, v_hours, v_inf, v_vent, v_misc,
                                      v_cop, v_shgc, v_s_hours, v_s_close):
    """Apply one study sample to an epJSON model."""
    f_wall = float(f_wall)
    f_roof = float(f_roof)
    f_win = float(f_win)
    v_set = float(v_set)
    v_hours = int(v_hours)
    v_inf = float(v_inf)
    v_vent = float(v_vent)
    v_misc = float(v_misc)
    v_cop = float(v_cop)
    v_shgc = float(v_shgc)
    v_s_hours = int(v_s_hours)
    v_s_close = int(v_s_close)

    target_zones = set(
        mapping_df[mapping_df["Cluster"] != "EXCLUDED"]["Zone_Name"].astype(str)
    )
    zones = model.get("Zone", {})
    surfaces = model.get("BuildingSurface:Detailed", {})
    constructions = model.get("Construction", {})
    materials = model.get("Material", {})
    simple_glazing = model.get("WindowMaterial:SimpleGlazingSystem", {})
    glazing = model.get("WindowMaterial:Glazing", {})
    modified_materials = set()

    for surface in surfaces.values():
        if surface.get("outside_boundary_condition", "").lower() != "outdoors":
            continue
        surface_type = surface.get("surface_type", "").lower()
        factor = f_wall if surface_type == "wall" else f_roof if surface_type in ["roof", "ceiling"] else 1.0
        construction = constructions.get(surface.get("construction_name"))
        if not construction:
            continue
        layers = [construction.get("outside_layer")]
        for index in range(2, 11):
            layer = construction.get(f"layer_{index}")
            if layer:
                layers.append(layer)
        for material_name in [layer for layer in layers[:-1] if layer]:
            if material_name in modified_materials:
                continue
            material = materials.get(material_name)
            conductivity = float(material.get("conductivity", 0.0)) if material else 0.0
            if material and conductivity > 0.05:
                material["conductivity"] = round(conductivity * factor, 4)
                if "density" in material:
                    material["density"] = round(float(material["density"]) * (1 + (factor - 1) * 0.5), 1)
                modified_materials.add(material_name)

    for surface in model.get("FenestrationSurface:Detailed", {}).values():
        if surface.get("surface_type", "").lower() != "window":
            continue
        parent = surfaces.get(surface.get("building_surface_name"))
        if not parent or str(parent.get("zone_name")) not in target_zones:
            continue
        construction = constructions.get(surface.get("construction_name"))
        if not construction:
            continue
        material_name = construction.get("outside_layer")
        if material_name in modified_materials:
            continue
        material = simple_glazing.get(material_name) or glazing.get(material_name)
        if material:
            if "conductivity" in material:
                material["conductivity"] = round(float(material["conductivity"]) * f_win, 4)
            if "u_factor" in material:
                material["u_factor"] = round(float(material["u_factor"]) * f_win, 3)
            modified_materials.add(material_name)

    schedules = model.setdefault("Schedule:Compact", {})
    shading_open, shading_close = _translate_shading(v_s_hours, v_s_close)
    schedules["Master_Shading_S"] = {
        "schedule_type_limits_name": "On/Off",
        "data": _schedule_fields([
            "Through: 31 Dec", "For: AllDays",
            f"Until: {shading_open}", v_shgc,
            f"Until: {shading_close}", 0.0,
            "Until: 24:00", v_shgc,
        ]),
    }
    shading_material_name = ""
    for control in model.get("WindowShadingControl", {}).values():
        shading_material_name = control.get("shading_device_material_name", shading_material_name)
        if shading_material_name:
            break
    for control in model.get("WindowShadingControl", {}).values():
        control["schedule_name"] = "Master_Shading_S"
        control["shading_control_type"] = "OnIfScheduleAllows"
        if shading_material_name and "shading_device_material_name" in control:
            control["shading_device_material_name"] = shading_material_name

    morning_start, morning_end, evening_start, evening_end = _translate_hours_to_strings(v_hours)
    schedules["Master_Heating_Setpoint_S"] = {
        "schedule_type_limits_name": "Temperature",
        "data": _schedule_fields([
            "Through: 31 Mar", "For: AllDays",
            f"Until: {morning_start}", 12.0, f"Until: {morning_end}", v_set,
            f"Until: {evening_start}", 12.0, f"Until: {evening_end}", v_set, "Until: 24:00", 12.0,
            "Through: 30 Nov", "For: AllDays", "Until: 24:00", 12.0,
            "Through: 31 Dec", "For: AllDays",
            f"Until: {morning_start}", 12.0, f"Until: {morning_end}", v_set,
            f"Until: {evening_start}", 12.0, f"Until: {evening_end}", v_set, "Until: 24:00", 12.0,
        ]),
    }
    schedules["Master_Heating_Availability_S"] = {
        "schedule_type_limits_name": "Fraction",
        "data": _schedule_fields([
            "Through: 31 Mar", "For: AllDays",
            f"Until: {morning_start}", 0.0, f"Until: {morning_end}", 1.0,
            f"Until: {evening_start}", 0.0, f"Until: {evening_end}", 1.0, "Until: 24:00", 0.0,
            "Through: 30 Nov", "For: AllDays", "Until: 24:00", 0.0,
            "Through: 31 Dec", "For: AllDays",
            f"Until: {morning_start}", 0.0, f"Until: {morning_end}", 1.0,
            f"Until: {evening_start}", 0.0, f"Until: {evening_end}", 1.0, "Until: 24:00", 0.0,
        ]),
    }
    for dual_setpoint in model.get("ThermostatSetpoint:DualSetpoint", {}).values():
        dual_setpoint["heating_setpoint_temperature_schedule_name"] = "Master_Heating_Setpoint_S"
    for hvac in model.get("ZoneHVAC:IdealLoadsAirSystem", {}).values():
        hvac["heating_availability_schedule_name"] = "Master_Heating_Availability_S"

    for item in model.get("ZoneInfiltration:DesignFlowRate", {}).values():
        zone = zones.get(item.get("zone_or_zonelist_name"))
        if zone and "volume" in zone:
            item["design_flow_rate_calculation_method"] = "Flow/Zone"
            item["design_flow_rate"] = (v_inf * float(zone["volume"])) / 3600.0
            item.pop("air_changes_per_hour", None)

    for item in model.get("ZoneVentilation:DesignFlowRate", {}).values():
        zone = zones.get(item.get("zone_or_zonelist_name"))
        if zone and "volume" in zone:
            item["design_flow_rate_calculation_method"] = "Flow/Zone"
            item["design_flow_rate"] = (v_vent * float(zone["volume"])) / 3600.0
            item.pop("air_changes_per_hour", None)

    for equipment in model.get("OtherEquipment", {}).values():
        equipment["design_level_calculation_method"] = "Watts/Area"
        equipment["power_per_zone_floor_area"] = v_misc
        equipment.pop("design_level", None)


def configure_heating_output_variables_epjson(model):
    """Set output variables to only report hourly heating energy."""
    model["Output:Variable"] = {
        "Output:Variable 1": {
            "key_value": "*",
            "variable_name": "Zone Ideal Loads Supply Air Total Heating Energy",
            "reporting_frequency": "Hourly",
        }
    }