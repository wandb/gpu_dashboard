import wandb
from tqdm import tqdm
import polars as pl
from src.utils.config import CONFIG
import logging

# ロギングの設定
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class RunExistenceChecker:
    def __init__(self):
        self.config = CONFIG
        self.api = wandb.Api()
        logging.info("RunExistenceChecker initialized")

    def get_artifact(self, entity, project, artifact_name):
        logging.debug(f"Fetching artifact: {entity}/{project}/{artifact_name}:latest")
        return self.api.artifact(f"{entity}/{project}/{artifact_name}:latest")

    def load_dataframe(self, artifact):
        logging.debug(f"Loading dataframe from artifact")
        df = pl.read_csv(artifact.file())
        if 'run_exists' not in df.columns:
            logging.info("Adding 'run_exists' column to dataframe")
            df = df.with_columns(pl.lit('exists').alias('run_exists'))
        logging.debug(f"Dataframe loaded, shape: {df.shape}")
        return df

    def check_run_existence(self, row):
        run_path = f"{row['company_name']}/{row['project']}/{row['run_id']}"
        try:
            self.api.run(run_path)
            return 'exists'
        except wandb.errors.CommError:
            logging.info(f"Run deleted: {run_path}")
            return 'deleted'
        except Exception as e:
            logging.error(f"Unexpected error occurred for run {run_path}: {str(e)}")
            return 'error'

    def update_run_existence(self, df):
        deleted_runs = []
        total_rows = df.shape[0]
        logging.info(f"Starting to check {total_rows} runs")
        with tqdm(total=total_rows, desc="Checking runs") as pbar:
            def update_progress(row):
                pbar.update(1)
                if row['run_exists'] == 'deleted':
                    return 'deleted'
                new_status = self.check_run_existence(row)
                if new_status == 'deleted' and row['run_exists'] != 'deleted':
                    deleted_runs.append(row)
                return new_status

            df = df.with_columns(
                pl.struct(df.columns)
                .map_elements(update_progress)
                .alias('run_exists')
            )
        logging.info(f"Run existence check completed. {len(deleted_runs)} runs were found to be newly deleted.")
        return df, deleted_runs

    def save_and_upload_artifact(self, df, csv_path, artifact_name, run):
        logging.info(f"Saving dataframe to {csv_path}")
        df.write_csv(csv_path)
        new_artifact = wandb.Artifact(name=artifact_name, type="dataset")
        new_artifact.add_file(csv_path)
        logging.info(f"Uploading artifact: {artifact_name}")
        run.log_artifact(new_artifact)

    def run_existence_check(self):
        artifact_name = self.config.dataset.artifact_name
        csv_path = f"{self.config.wandb_dir}/{artifact_name}.csv"
        deleted_runs_artifact_name = self.config.dataset.deleted_runs_artifact_name

        logging.info("Starting run existence check")
        with wandb.init(
            entity=self.config.dashboard.entity,
            project=self.config.dashboard.project,
            name="run_existence_check", 
            job_type="data_update") as run:
            try:
                logging.info("Fetching artifact")
                artifact = self.get_artifact(self.config.dataset.entity, self.config.dataset.project, artifact_name)
                logging.info("Loading dataframe")
                df = self.load_dataframe(artifact)
                logging.info("Updating run existence")
                df, deleted_runs = self.update_run_existence(df)
                logging.info("Saving and uploading updated artifact")
                self.save_and_upload_artifact(df, csv_path, artifact_name, run)
                
                # 削除が検知されたrunの情報を保存し、アップロード
                logging.info("Processing deleted runs")
                deleted_runs_df = pl.DataFrame(deleted_runs) if deleted_runs else pl.DataFrame({"run_id": [], "company_name": [], "project": [], "created_at": []})
                deleted_runs_csv_path = f"{self.config.wandb_dir}/{deleted_runs_artifact_name}.csv"
                deleted_runs_df.write_csv(deleted_runs_csv_path)
                
                deleted_runs_artifact = wandb.Artifact(name=deleted_runs_artifact_name, type="dataset")
                deleted_runs_artifact.add_file(deleted_runs_csv_path)
                logging.info("Uploading deleted runs artifact")
                run.log_artifact(deleted_runs_artifact)

                logging.info("Process completed. Updated CSV files have been uploaded as new artifacts.")
            except Exception as e:
                logging.error(f"An error occurred: {str(e)}")
                wandb.log({"error": str(e)})

if __name__ == "__main__":
    checker = RunExistenceChecker()
    checker.run_existence_check()