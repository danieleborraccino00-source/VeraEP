# EnergyPlus LHS Study on Vera

![Platform](https://img.shields.io/badge/platform-Vera%20%2F%20C3SE-0A6EBD)
![Scheduler](https://img.shields.io/badge/scheduler-SLURM-1F883D)
![EnergyPlus](https://img.shields.io/badge/EnergyPlus-9.4.0-8A2BE2)
![Config](https://img.shields.io/badge/config-JSON-F59E0B)
![Workflow](https://img.shields.io/badge/workflow-batched%20LHS-6B7280)

This repository runs a batched EnergyPlus Latin Hypercube Sampling study on the Vera cluster at C3SE.

It is set up for reuse: users bring their own IDF and EPW files, local machine-specific settings live in a simple JSON config, and generated study artifacts are kept out of Git.

## Repository At A Glance

The root is intentionally organized around user-facing entrypoints:

- `project_config.json`: tracked shared defaults
- `project_config.local.example.json`: tracked template for per-user overrides
- `setup_environment.sh`: first-run bootstrap and validation
- `run_study.sh`: main orchestrator for the 3-phase batch run
- `plan_study.sh`: planning-only report
- `monitor.sh`: compact live study monitor
- `clean_workspace.sh`: destructive local cleanup helper

The real implementation lives under `eplus_study/` and `slurm/`:

- `eplus_study/config.py`: shared config loader and CLI helper
- `eplus_study/generate_samples.py`: Phase 1 implementation
- `eplus_study/simulate_batch.py`: Phase 2 implementation
- `eplus_study/consolidate_outputs.py`: Phase 3 implementation
- `eplus_study/idf_parametrics.py`: LHS sampling, zone mapping, and IDF-side parametric helpers
- `eplus_study/epjson_parametrics.py`: epJSON cache and per-simulation model edits
- `slurm/generate.sh`, `slurm/worker.sh`, `slurm/consolidate.sh`: actual SLURM payloads

The user-facing shell commands stay at the root, and the implementation stays grouped internally.

## Fresh Clone Checklist

For a new user on a clean clone, the intended setup is:

1. Clone the repo.
2. Copy `project_config.local.example.json` to `project_config.local.json`.
3. Edit `project_config.local.json` with your SLURM account and your input file paths.
4. Put your own IDF and EPW files at those configured paths.
5. Run `bash setup_environment.sh`.
6. Preview with `PLAN_ONLY=1 bash run_study.sh 5000`.
7. Launch with `bash run_study.sh 5000`.
8. Monitor with `bash monitor.sh --watch 5`.

The only local files a new user must supply are:

- their own IDF
- their own EPW
- their own ignored `project_config.local.json`

## What To Edit

The main shared settings live in `project_config.json`.

If you want user-specific overrides without editing the tracked defaults, create `project_config.local.json`. That file is ignored by Git.

The easiest way to start is:

```bash
cp project_config.local.example.json project_config.local.json
```

Typical things to override are:

- `project.study_dir`
- `inputs.idf_path`
- `inputs.epw_path`
- `inputs.epjson_cache_path`
- `energyplus.install_dir`
- `energyplus.download_url`
- `slurm.account`
- `slurm.partition`

Minimal example:

```json
{
	"slurm": {
		"account": "YOUR_PROJECT"
	},
	"inputs": {
		"idf_path": "idf/my_building.idf",
		"epw_path": "epw/my_weather.epw"
	}
}
```

If `inputs.epjson_cache_path` is omitted, it is derived automatically from `inputs.idf_path` by replacing the file suffix with `.epJSON`.

The default study output directory is `results/`.

`project_config.local.example.json` is tracked. `project_config.local.json` is ignored. That means each user can keep their own local paths and account settings without polluting Git.

## Quick Start on Vera

```bash
ssh YOUR_CID@vera1.c3se.chalmers.se
cd /cephyr/users/YOUR_CID/Vera
git clone YOUR_REPO_URL eplus_project
cd eplus_project
```

Place your own input files at the paths configured in `project_config.json` or `project_config.local.json`.

In other words, a clean clone is expected to contain the tracked repo files only. The IDF, EPW, and `project_config.local.json` are the user-specific local additions.

Then run the one-time bootstrap:

```bash
bash setup_environment.sh
```

That script will:

- create or reuse the virtual environment
- install Python dependencies
- download EnergyPlus if it is missing
- validate the configured IDF, EPW, SLURM tools, and EnergyPlus install
- generate or refresh the cached baseline epJSON
- tell you when the workspace is ready for a large batch run

## Run a Study

Preview the launch plan:

```bash
PLAN_ONLY=1 bash run_study.sh 5000
```

Launch the study:

```bash
bash run_study.sh 5000
```

If the configured study directory already contains outputs from a previous run, the launcher will list what will be erased and require you to type `y` before it submits jobs.

Monitor the run:

```bash
bash monitor.sh --watch 5
```

## Clean the Workspace

If you want to sanitize the workspace before sharing it or starting fresh, use:

```bash
bash clean_workspace.sh --dry-run
bash clean_workspace.sh
```

This script is intentionally destructive. It warns first and then removes local-only artifacts such as:

- study outputs
- local IDF, EPW, and cached epJSON files
- archives and logs
- the virtual environment
- the EnergyPlus install and archive
- `legacy/`

If you run `clean_workspace.sh`, you must put your IDF and EPW files back before the next setup or study run.

## Manual Phase Submission

If you do not want to use `run_study.sh`, generate the `sbatch` arguments from the config first:

```bash
mapfile -t GENERATE_ARGS < <(python3 -m eplus_study.config sbatch-args generate)
mapfile -t WORKER_ARGS < <(python3 -m eplus_study.config sbatch-args worker)
mapfile -t CONSOLIDATE_ARGS < <(python3 -m eplus_study.config sbatch-args consolidate)
```

Those arrays can then be passed into `sbatch` with `slurm/generate.sh`, `slurm/worker.sh`, and `slurm/consolidate.sh`.

## Git Notes

This repository ignores local-only files such as:

- `idf/` contents
- `epw/` contents
- `project_config.local.json`
- `results/` outputs
- `archives/`
- `logs/`
- `venv/`
- EnergyPlus installs and tarballs

If any of those files were already tracked before you added these ignore rules, remove them from the Git index once:

```bash
git rm --cached -r idf epw results archives logs
```

The current `gitignore` is already set up correctly for the local-config workflow:

- `project_config.local.json` is ignored
- `project_config.local.example.json` is tracked
- local IDF, EPW, outputs, logs, archives, and EnergyPlus artifacts are ignored

## More Detail

For the full workflow, quotas, planning, and phase-by-phase details, see `instructions.md`.