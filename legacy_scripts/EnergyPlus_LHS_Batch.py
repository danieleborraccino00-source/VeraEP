"""
Original EnergyPlus LHS parametric study script.

Original Author: Daniele Borraccino <borraccino1@phd.poliba.it>
Politecnico di Bari

This is the original script that served as the basis for the optimized batch pipeline.
See eplus_study/ package for the production implementation.
"""
import os
import shutil
import pandas as pd
import subprocess
from eppy.modeleditor import IDF
import numpy as np
from scipy.stats import qmc

# 1. CONFIGURATION
# Note: This script was originally developed and tested with EnergyPlus version 9.4.0
# Set this to your local EnergyPlus installation directory (e.g., V9-4-0 or V22-2-0)
EP_INSTALL_PATH = r'C:\EnergyPlusV9-4-0' 
EP_EXE = os.path.join(EP_INSTALL_PATH, 'energyplus.exe')
working_dir = os.getcwd() # Automatically gets the folder where this .py file is saved
base_path = working_dir 
idd_file = os.path.join(EP_INSTALL_PATH, 'Energy+.idd')
IDF.setiddname(idd_file)

# PARAMETERS
epw_file = os.path.join(base_path, 'Bari.epw')
idf_baseline_path = os.path.join(base_path, 'Baseline_Shading.idf')

# AUTOMATIC MAPPING GENERATION FROM IDF FILE ---
def generate_mapping(idf_path):
    temp_idf = IDF(idf_path)

    # Identify heated zones via thermostats (using Zone_or_ZoneList_Name field)
    hvac_zones = [t.Zone_or_ZoneList_Name for t in temp_idf.idfobjects['ZONECONTROL:THERMOSTAT']]

    mapping_data = []
    for zone in temp_idf.idfobjects['ZONE']:
        name = zone.Name
        is_heated = name in hvac_zones

        # Calculate Area and Z-Height (needed for final results and clustering)
        # Search for 'Floor' surfaces associated with this zone
        surfaces = [s for s in temp_idf.idfobjects['BUILDINGSURFACE:DETAILED']
                    if s.Zone_Name == name and s.Surface_Type.lower() == 'floor']

        floor_area = 0
        z_coords = []
        for s in surfaces:
            floor_area += s.area # eppy automatically calculates area from vertices
            z_coords.append(sum([v[2] for v in s.coords]) / len(s.coords)) # Average Z

        avg_z = sum(z_coords) / len(z_coords) if z_coords else 0
        if floor_area == 0: floor_area = 20.0 # Safety fallback

        mapping_data.append({
            'Zone_Name': name,
            'Is_Heated': is_heated,
            'Z_Level': round(avg_z, 2),
            'Area_sqm': round(floor_area, 2)
        })

    df = pd.DataFrame(mapping_data)

    # Cluster Creation (Ground, Middle, Top)
    if not df[df['Is_Heated']].empty:
        heated_df = df[df['Is_Heated']]
        min_z, max_z = heated_df['Z_Level'].min(), heated_df['Z_Level'].max()

        def classify(row):
            if not row['Is_Heated']: return 'EXCLUDED'
            if row['Z_Level'] == min_z: return 'GROUND'
            if row['Z_Level'] == max_z: return 'TOP'
            return 'MIDDLE'

        df['Cluster'] = df.apply(classify, axis=1)
    else:
        df['Cluster'] = 'EXCLUDED'

    mapping_path = os.path.join(working_dir, 'auto_mapping.csv')
    df.to_csv(mapping_path, index=False)
    print(f" Mapping generated successfully: {len(df)} zones analyzed.")
    return mapping_path

# Generate file and save path
mapping_drive_path = generate_mapping(idf_baseline_path)

# Create folder for 1000 LHS results
study_folder = os.path.join(working_dir, "LHS_LOCAL_STUDY")

if os.path.exists(study_folder): shutil.rmtree(study_folder) # Optional: clean if exists
os.makedirs(study_folder, exist_ok=True)

# Load generated mapping for final calculations
mapping_df = pd.read_csv(mapping_drive_path)

# Dynamically calculate number of zones per cluster
n_ground = len(mapping_df[mapping_df['Cluster'] == 'GROUND'])
n_middle = len(mapping_df[mapping_df['Cluster'] == 'MIDDLE'])
n_top    = len(mapping_df[mapping_df['Cluster'] == 'TOP'])

print(f"Mapped Building: {n_ground} ground zones, {n_middle} middle, {n_top} top.")

# 2. HELPER FUNCTIONS
def get_real_u(idf_obj, surface_type):
    """Calculates real thermal transmittance (U) for modified opaque components"""
    try:
        surf = [s for s in idf_obj.idfobjects['BUILDINGSURFACE:DETAILED']
                if s.Surface_Type.lower() == surface_type.lower() and s.Outside_Boundary_Condition.lower() == 'outdoors'][0]
        c = idf_obj.getobject('CONSTRUCTION', surf.Construction_Name)
        layers = [c.Outside_Layer] + [getattr(c, f"Layer_{i}", None) for i in range(2, 11) if getattr(c, f"Layer_{i}", None)]
        r_tot = 0.17 # Standard surface resistances (Rse + Rsi)
        for m_name in [l for l in layers if l]:
            m = idf_obj.getobject('MATERIAL', m_name)
            if m: r_tot += float(m.Thickness) / float(m.Conductivity)
        return round(1 / r_tot, 3)
    except: return 0.0

def translate_hours_to_strings(h):
    """Converts LHS extracted hours into precise IDF schedule times"""
    h = int(h)
    em, es = 10, 22 # Activation limits (morning and evening)
    if h <= 4:
        return "08:00", "08:00", f"{es-h:02d}:00", f"{es}:00"
    elif h <= 8:
        return f"{em-(h-4):02d}:00", f"{em}:00", "18:00", "22:00"
    else:
        return "06:00", "10:00", f"{es-(h-4):02d}:00", f"{es}:00"

def translate_shading(h_open, h_close):
    h_close = int(h_close)
    h_open_time = h_close - int(h_open)
    return f"{h_open_time:02d}:00", f"{h_close:02d}:00"

# 3. LHS MATRIX GENERATION
def generate_lhs(n):
    # Range definitions: (Min, Max, Step)
    ranges = {
        'f_wall':   (1.0, 1.5, 0.01),   
        'f_roof':   (1.0, 1.5, 0.01),
        'f_win':    (1.0, 1.3, 0.01),
        'setpoint': (17.0, 24.0, 0.1),  
        'hours':    (2, 14, 1),         
        'inf':      (0.3, 1.5, 0.05),   
        'vent':     (0.0, 0.8, 0.05),   
        'misc':     (1.0, 6.0, 0.2),    
        'shgc':     (0.1, 0.9, 0.05),   
        's_hours':  (4, 11, 1),   
        's_close':  (16, 20, 1),  
        'cop':      (0.65, 0.95, 0.05)
    }

    # Latin Hypercube sampler for dimensions
    sampler = qmc.LatinHypercube(d=len(ranges), seed=42)
    sample_raw = sampler.random(n)

    # Scaling to defined min/max
    mins = np.array([v[0] for v in ranges.values()])
    maxs = np.array([v[1] if v[1] > v[0] else v[1] + 1e-7 for v in ranges.values()])
    sample_scaled = qmc.scale(sample_raw, mins, maxs)

    # Rounding based on steps
    lhs_list = []
    for row in sample_scaled:
        rounded_row = {}
        for i, (name, v) in enumerate(ranges.items()):
            step = v[2]
            rounded_row[name] = round(row[i] / step) * step
        lhs_list.append(rounded_row)

    return pd.DataFrame(lhs_list).round(3)

# Generation of cases
n_sim = 1000
lhs_df = generate_lhs(n_sim)
pd.set_option('display.max_rows', None) 
print(lhs_df.to_string()) 


# 4. INTEGRATED AGING FUNCTION
def apply_complete_aging(idf_obj, f_wall, f_roof, f_win, v_set, v_hours, v_inf, v_vent, v_misc, v_cop, v_shgc, v_s_hours, v_s_close):
    target_zones = mapping_df[mapping_df['Cluster'] != 'EXCLUDED']['Zone_Name'].astype(str).tolist()
    modified_materials = set()
    modified_log = []

# --- PART A: WALLS AND ROOFS ---
    for s in idf_obj.idfobjects['BUILDINGSURFACE:DETAILED']:
        if s.Outside_Boundary_Condition.lower() == 'outdoors':
            stype = s.Surface_Type.lower()
            factor = f_wall if stype == 'wall' else f_roof if stype in ['roof', 'ceiling'] else 1.0
            constr = idf_obj.getobject('CONSTRUCTION', s.Construction_Name)
            layers = [constr.Outside_Layer] + [getattr(constr, f"Layer_{i}", None) for i in range(2, 11) if getattr(constr, f"Layer_{i}", None)]
            layers = [l for l in layers if l is not None]
            for i in range(len(layers) - 1):
                mat_name = layers[i]
                if mat_name not in modified_materials:
                    mat = idf_obj.getobject('MATERIAL', mat_name)
                    if mat and mat.Conductivity > 0.05:
                        old_c, old_d = mat.Conductivity, mat.Density
                        mat.Conductivity = round(old_c * factor, 4)
                        mat.Density = round(old_d * (1 + (factor - 1) * 0.5), 1)
                        modified_log.append({'TYPE': stype.upper(), 'MAT_ID': mat_name, 'LAYER': f"Layer {i+1}", 'THICKNESS': mat.Thickness, 'OLD_VAL': old_c, 'NEW_VAL': mat.Conductivity, 'OLD_DENS': old_d, 'NEW_DENS': mat.Density})
                        modified_materials.add(mat_name)

    # --- PART B: WINDOWS ---
    for s in idf_obj.idfobjects['FENESTRATIONSURFACE:DETAILED']:
        if s.Surface_Type.lower() == 'window':
            if parent_surf and parent_surf.Zone_Name in target_zones:
                constr = idf_obj.getobject('CONSTRUCTION', s.Construction_Name)
                mat_name = constr.Outside_Layer
                if mat_name not in modified_materials:
                    mat = (idf_obj.getobject('WINDOWMATERIAL:SIMPLEGLAZINGSYSTEM', mat_name) or idf_obj.getobject('WINDOWMATERIAL:GLAZING', mat_name))
                    if mat:
                        old_v = getattr(mat, 'Conductivity', getattr(mat, 'U_Factor', '-'))
                        if hasattr(mat, 'Conductivity'): mat.Conductivity = round(mat.Conductivity * f_win, 4)
                        if hasattr(mat, 'U_Factor'): mat.U_Factor = round(mat.U_Factor * f_win, 3)
                        modified_log.append({'TYPE': 'WINDOW', 'MAT_ID': mat_name, 'LAYER': 'Glass', 'THICKNESS': '-', 'OLD_VAL': old_v, 'NEW_VAL': getattr(mat, 'Conductivity', getattr(mat, 'U_Factor', '-')), 'OLD_DENS': '-', 'NEW_DENS': '-'})
                        modified_materials.add(mat_name)

    # --- PART C: SHADING ---
    ctrls = idf_obj.idfobjects['WINDOWSHADINGCONTROL']
    sh_mat = ctrls[0].Shading_Device_Material_Name if ctrls else ""
    old_sh = ctrls[0].Schedule_Name if ctrls else "N/A"
    sh_sch = idf_obj.newidfobject('SCHEDULE:COMPACT', Name="Master_Shading_S", Schedule_Type_Limits_Name="On/Off")
    h_open_t, h_close_t = translate_shading(v_s_hours, v_s_close)
    f_sh = ["Through: 31 Dec", "For: AllDays", f"Until: {h_open_t}", str(v_shgc), f"Until: {h_close_t}", "0.0", "Until: 24:00", str(v_shgc)]
    for i, v in enumerate(f_sh): sh_sch[f"Field_{i+1}"] = v
    for ctrl in ctrls: ctrl.Schedule_Name, ctrl.Shading_Control_Type, ctrl.Shading_Device_Material_Name = "Master_Shading_S", "OnIfScheduleAllows", sh_mat
    modified_log.append({'TYPE': 'BEHAVIOR', 'MAT_ID': 'Shading_Sch', 'LAYER': 'Shading', 'THICKNESS': '-', 'OLD_VAL': old_sh, 'NEW_VAL': f'Open {h_open_t}-{h_close_t}', 'OLD_DENS': '-', 'NEW_DENS': '-'})


    # --- PART D: HEATING ---
    h1, h2, h3, h4 = translate_hours_to_strings(v_hours)
    try:
        old_set_n = idf_obj.idfobjects['THERMOSTATSETPOINT:DUALSETPOINT'][0].Heating_Setpoint_Temperature_Schedule_Name
        old_sch_o = idf_obj.getobject('SCHEDULE:COMPACT', old_set_n) or idf_obj.getobject('SCHEDULE:CONSTANT', old_set_n)
        old_temp = old_sch_o.fieldvalues[5] if hasattr(old_sch_o, 'fieldvalues') and len(old_sch_o.fieldvalues) > 5 else "Original"
    except: old_temp = "Original"

    s_name, a_name = "Master_Heating_Setpoint_S", "Master_Heating_Availability_S"
    for n in [s_name, a_name]:
        for old_s in [x for x in idf_obj.idfobjects['SCHEDULE:COMPACT'] if x.Name == n]: idf_obj.removeidfobject(old_s)

    sch_s = idf_obj.newidfobject('SCHEDULE:COMPACT', Name=s_name, Schedule_Type_Limits_Name="Temperature")
    fields_s = ["Through: 31 Mar", "For: AllDays", f"Until: {h1}", "12.0", f"Until: {h2}", str(v_set), f"Until: {h3}", "12.0", f"Until: {h4}", str(v_set), "Until: 24:00", "12.0",
                "Through: 30 Nov", "For: AllDays", "Until: 24:00", "12.0",
                "Through: 31 Dec", "For: AllDays", f"Until: {h1}", "12.0", f"Until: {h2}", str(v_set), f"Until: {h3}", "12.0", f"Until: {h4}", str(v_set), "Until: 24:00", "12.0"]
    for i, v in enumerate(fields_s): sch_s[f"Field_{i+1}"] = v

    sch_a = idf_obj.newidfobject('SCHEDULE:COMPACT', Name=a_name, Schedule_Type_Limits_Name="Fraction")
    fields_a = ["Through: 31 Mar", "For: AllDays", f"Until: {h1}", "0.0", f"Until: {h2}", "1.0", f"Until: {h3}", "0.0", f"Until: {h4}", "1.0", "Until: 24:00", "0.0",
                "Through: 30 Nov", "For: AllDays", "Until: 24:00", "0.0",
                "Through: 31 Dec", "For: AllDays", f"Until: {h1}", "0.0", f"Until: {h2}", "1.0", f"Until: {h3}", "0.0", f"Until: {h4}", "1.0", "Until: 24:00", "0.0"]
    for i, v in enumerate(fields_a): sch_a[f"Field_{i+1}"] = v

    for dsp in idf_obj.idfobjects['THERMOSTATSETPOINT:DUALSETPOINT']: dsp.Heating_Setpoint_Temperature_Schedule_Name = s_name
    for hvac in idf_obj.idfobjects['ZONEHVAC:IDEALLOADSAIRSYSTEM']: hvac.Heating_Availability_Schedule_Name = a_name
    modified_log.append({'TYPE': 'BEHAVIOR', 'MAT_ID': 'Heating', 'LAYER': 'Sched/Setp', 'THICKNESS': '-', 'OLD_VAL': f"{old_temp} C", 'NEW_VAL': f"{v_set}C ({h1}-{h4})", 'OLD_DENS': '-', 'NEW_DENS': '-'})

    # --- PART E: AIR AND LOADS ---
    vol_ref = float(idf_obj.idfobjects['ZONE'][0].Volume)
    inf_obj = idf_obj.idfobjects['ZONEINFILTRATION:DESIGNFLOWRATE'][0] if idf_obj.idfobjects['ZONEINFILTRATION:DESIGNFLOWRATE'] else None
    old_inf = f"{round((float(inf_obj.Design_Flow_Rate)*3600)/vol_ref, 2)} ACH" if inf_obj and inf_obj.Design_Flow_Rate != "" else "N/A"

    for item in idf_obj.idfobjects['ZONEINFILTRATION:DESIGNFLOWRATE']:
        z = idf_obj.getobject('ZONE', item.Zone_or_ZoneList_Name)
        if z: item.Design_Flow_Rate_Calculation_Method, item.Design_Flow_Rate, item.Air_Changes_per_Hour = 'Flow/Zone', (v_inf * float(z.Volume)) / 3600.0, ""
    modified_log.append({'TYPE': 'BEHAVIOR', 'MAT_ID': 'Infiltration', 'LAYER': 'AirChanges', 'THICKNESS': '-', 'OLD_VAL': old_inf, 'NEW_VAL': f"{v_inf} ACH", 'OLD_DENS': '-', 'NEW_DENS': '-'})

    for item in idf_obj.idfobjects['ZONEVENTILATION:DESIGNFLOWRATE']:
        z = idf_obj.getobject('ZONE', item.Zone_or_ZoneList_Name)
        if z: item.Design_Flow_Rate_Calculation_Method, item.Design_Flow_Rate, item.Air_Changes_per_Hour = 'Flow/Zone', (v_vent * float(z.Volume)) / 3600.0, ""
    modified_log.append({'TYPE': 'BEHAVIOR', 'MAT_ID': 'Ventilation', 'LAYER': 'FreshAir', 'THICKNESS': '-', 'OLD_VAL': '0.5 ACH', 'OLD_VAL': f"{v_vent} ACH", 'OLD_DENS': '-', 'NEW_DENS': '-'})

    old_misc = idf_obj.idfobjects['OTHEREQUIPMENT'][0].Power_per_Zone_Floor_Area if idf_obj.idfobjects['OTHEREQUIPMENT'] else "-"
    for eq in idf_obj.idfobjects['OTHEREQUIPMENT']:
        eq.Design_Level_Calculation_Method, eq.Power_per_Zone_Floor_Area, eq.Design_Level = 'Watts/Area', v_misc, ""
    modified_log.append({'TYPE': 'BEHAVIOR', 'MAT_ID': 'MiscGains', 'LAYER': 'Watts/sqm', 'THICKNESS': '-', 'OLD_VAL': f"{old_misc}", 'NEW_VAL': f"{v_misc}", 'OLD_DENS': '-', 'NEW_DENS': '-'})

    modified_log.append({'TYPE': 'SYSTEM', 'MAT_ID': 'Boiler', 'LAYER': 'COP', 'THICKNESS': '-', 'OLD_VAL': '0.75', 'NEW_VAL': v_cop, 'OLD_DENS': '-', 'NEW_DENS': '-'})

    return pd.DataFrame(modified_log)


# --- 5. STUDY PREPARATION ---
csv_results_path = os.path.join(study_folder, "FINAL_LHS_RESULTS.csv")

# --- SIMULATION LOOP ---
print(f" PHASE 1: GENERATING {n_sim} IDF FILES")
for i, row in lhs_df.iterrows():
    sim_id = i + 1
    run_dir = os.path.join(study_folder, f"Run_{sim_id}")
    os.makedirs(run_dir, exist_ok=True)

    # Load baseline IDF and apply aging logic
    idf = IDF(idf_baseline_path)
    apply_complete_aging(idf, row['f_wall'], row['f_roof'], row['f_win'], row['setpoint'], 
                         row['hours'], row['inf'], row['vent'], row['misc'], row['cop'], 
                         row['shgc'], row['s_hours'], row['s_close'])

    # Add output variables
    for o in list(idf.idfobjects['OUTPUT:VARIABLE']): idf.removeidfobject(o)
    idf.newidfobject("OUTPUT:VARIABLE", Key_Value="*", 
                    Variable_Name="Zone Ideal Loads Supply Air Total Heating Energy", 
                    Reporting_Frequency="Hourly")

    # Save IDF to specific folder
    idf_path = os.path.join(run_dir, "in.idf")
    idf.saveas(idf_path)
    if sim_id % 10 == 0: print(f" Generated {sim_id}/{n_sim} files...")

print(f"\n PHASE 2: RUNNING ENERGYPLUS SIMULATIONS")
for i, row in lhs_df.iterrows():
    sim_id = i + 1
    run_dir = os.path.join(study_folder, f"Run_{sim_id}")
    idf_path = os.path.join(run_dir, "in.idf")

    print(f" Running Case {sim_id}/{n_sim}...", end="\r")

    # Execute EnergyPlus
    subprocess.run([EP_EXE, "-w", epw_file, "-d", run_dir, "-p", "eplus", "-r", idf_path], capture_output=True)

    # Extract and save results (same as previous system)
    csv_path = os.path.join(run_dir, "eplusout.csv")
    if os.path.exists(csv_path):
        df_res = pd.read_csv(csv_path)
        heat_cols = [c for c in df_res.columns if "Heating Energy" in c]
        final_list = []
        for _, zone in mapping_df.iterrows():
            if zone['Is_Heated']:
                z_id = str(zone['Zone_Name'])
                cols = [c for c in heat_cols if z_id in c]
                if cols:
                    tot_kwh = (df_res[cols].sum().sum() / 3600000.0) / row['cop']
                    final_list.append({'Unit': z_id, 'Floor': zone['Cluster'], 'kWh_sqm': round(tot_kwh / zone['Area_sqm'], 2)})

        # Save aggregated data to final CSV
        res_row = row.to_dict()
        res_row['SIM_ID'] = sim_id
        df_temp = pd.DataFrame(final_list)
        if not df_temp.empty:
            for level in ['GROUND', 'MIDDLE', 'TOP']:
                res_row[f'kWh_sqm_{level}'] = round(df_temp[df_temp['Floor'] == level]['kWh_sqm'].mean(), 2)

        pd.DataFrame([res_row]).to_csv(csv_results_path, mode='a', index=False, header=not os.path.exists(csv_results_path))

print(f"\n STUDY COMPLETED! Results are in: {csv_results_path}")