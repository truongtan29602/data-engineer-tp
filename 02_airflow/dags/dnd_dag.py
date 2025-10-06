import json
import glob
import random
import requests
import pendulum
import pandas as pd
import logging
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)
api_url = "https://www.dnd5eapi.co/api/2014"
output_folder = "/opt/airflow/data"

# !) Failure callback for console logs
def failure_alert(context):
    exc = context.get("exception")
    ti = context.get("ti")
    logger.error(
        f'FAILED task: dag={getattr(ti, "dag_id")} task_id={getattr(ti, "task_id")} run_id={context.get("run_id")} try={getattr(ti, "try_number")}'
    )
    # Full traceback in the task log:
    logger.exception(exc)

START_DATE = pendulum.datetime(2025, 10, 1, tz="UTC")

with DAG(
    dag_id = "dnd_dag",
    start_date = START_DATE,
    schedule = None,
    catchup = False,
    max_active_tasks = 1,
    default_args = {
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
        "on_failure_callback": failure_alert,
    },
    template_searchpath=["/opt/airflow/data/"],
    tags=["homework"]
) as dag:
    # ---------- HELPERS ----------
    def _name_race(output_folder: str, endpoint: str, url: str):
        from faker import Faker
        fake = Faker()
        try:
            logger.info(f"Fetching {endpoint} from {url}/{endpoint} ...")
            response = requests.get(url = f"{url}/{endpoint}", timeout = 30)
            data = response.json()
            races = random.sample([race.get("index") for race in data.get("results", [])], 5)
            names = [fake.first_name() for _ in range(5)]
            with open(f"{output_folder}/{endpoint}.json", "w") as f:
                json.dump({"races": races}, f, ensure_ascii = False)
            with open(f"{output_folder}/names.json", "w") as f:
                json.dump({"names": names}, f, ensure_ascii = False)
        except Exception as e:
            logger.exception("Error in _name_race")
            raise
    
    def _attributes(output_folder: str, endpoint: str, url: str):
        try:
            logger.info(f"Generating {endpoint} ...")
            attributes = []
            for _ in range(5):
                instance = [random.randint(6,18)] + [random.randint(2,18) for _ in range(4)]
                attributes.append(instance)
            with open(f"{output_folder}/{endpoint}.json", "w") as f:
                json.dump({"attributes": attributes}, f, ensure_ascii = False)
        except Exception as e:
            logger.exception("Error in _attributes")
            raise

    def _languages(output_folder: str, endpoint: str, url: str):
        try:
            logger.info(f"Fetching {endpoint} from {url}/{endpoint} ...")
            response = requests.get(url = f"{url}/{endpoint}", timeout = 30)
            data = response.json()
            picked_languages = random.sample([language.get("index") for language in data.get("results", [])], 5)
            with open(f"{output_folder}/{endpoint}.json", "w") as f:
                json.dump({"languages": picked_languages}, f, ensure_ascii = False)
            logger.info(f"Picked {len(picked_languages)} languages")
        except Exception as e:
            logger.exception("Error in _languages")
            raise

    def _classes(output_folder: str, endpoint: str, url: str):
        try:
            logger.info(f"Fetching {endpoint} from {url}/{endpoint} ...")
            response = requests.get(url = f"{url}/{endpoint}", timeout = 30)
            data = response.json()
            cls = random.sample([cl.get("index") for cl in data.get("results", [])], 5)
            with open(f"{output_folder}/{endpoint}.json", "w") as f:
                json.dump({"classes": cls}, f, ensure_ascii = False)
            logger.info(f"Picked {len(cls)} classes")
        except Exception as e:
            logger.exception("Error in _classes")
            raise

    def _proficiency_choices(output_folder: str, endpoint: str, url: str):
        try:
            with open(f"{output_folder}/classes.json", "r") as read_file:
                data = json.load(read_file)
            load_classes = data.get("classes", [])
            logger.info(f"Computing proficiencies for {len(load_classes)} classes")

            def get_proficiencies(cls):
                d = requests.get(f"{url}/classes/{cls}", timeout=30).json()
                first_choice = (d.get("proficiency_choices") or [{}])[0]
                options = (first_choice.get("from").get("options") or [])   # <-- fixed name
                choose_n = int(first_choice.get("choose") or 0)
                picked = random.sample([i.get("item").get("index") for i in options], choose_n) if choose_n and options else []
                logger.debug("class=%s choose=%s -> %s", cls, choose_n, picked)
                return picked
            
            final_list = [get_proficiencies(c) for c in load_classes]

            with open(f"{output_folder}/{endpoint}.json", "w") as f:
                json.dump({"proficiences": final_list}, f, ensure_ascii = False)
            logger.info(f"Picked {len(final_list)} proficiency choices")
        except Exception as e:
            logger.exception("Error in _proficiency_choices")
            raise

    def _levels(output_folder: str, endpoint: str, url: str):
        try:
            random_levels = [random.randint(1, 3) for _ in range(5)]
            with open(f"{output_folder}/{endpoint}.json", "w") as f:
                json.dump({"levels": random_levels}, f, ensure_ascii=False)
            logger.info(f"Generated levels: {random_levels}")
        except Exception:
            logger.exception("Error in _levels")
            raise

    def _spell_check(output_folder: str, url: str):
        try:
            with open(f"{output_folder}/classes.json", "r") as read_file:
                data = json.load(read_file)
            load_classes = data.get("classes", [])
            logger.info(f"Checking spell counts for {len(load_classes)} classes")

            def spellcount(cls):
                logger.info(f"Spells url: {url}/classes/{cls}/spells")
                response = requests.get(f"{url}/classes/{cls}/spells", timeout=30).json()
                return int(response.get("count") or 0)

            total = sum(spellcount(c) for c in load_classes)
            logger.info(f"Total spell count across classes: {total}")
            return "spells" if total > 0 else "merge"
        except Exception:
            logger.exception("Error in _spell_check")
            raise

    def _spells(output_folder: str, endpoint: str, url: str):
        try:
            with open(f"{output_folder}/classes.json", "r") as read_file:
                data = json.load(read_file)
            load_classes = data.get("classes", [])
            logger.info(f"Fetching spells for {len(load_classes)} classes")

            def get_spells(cls):
                resp = requests.get(f"{url}/classes/{cls}/spells", timeout=30).json()
                results = resp.get("results", [])
                pick = random.randint(1, min(3, len(results))) if results else 0
                chosen = random.sample([i.get("index") for i in results], pick) if pick else []
                logger.debug(f"Class={cls} picked {pick} spells -> {chosen}")
                return chosen

            spell_lists = [get_spells(c) for c in load_classes]
            with open(f"{output_folder}/{endpoint}.json", "w") as f:
                json.dump({"spells": spell_lists}, f, ensure_ascii=False)
            logger.info("Wrote spells list")
        except Exception:
            logger.exception("Error in _spells")
            raise

    def _merge(output_folder: str, endpoint: str, url: str):
        try:
            jsons = sorted(glob.glob(f"{output_folder}/*.json"))
            logger.info(f"Merging {len(jsons)} JSON node files")
            dfs = [pd.read_json(p) for p in jsons]
            df = pd.concat(dfs, axis=1) if dfs else pd.DataFrame()
            df.to_csv(f"{output_folder}/{endpoint}.csv", index=False)
            logger.info(f"Wrote {output_folder}/{endpoint}.csv with {df.shape[0] if not df.empty else 0} rows, {df.shape[1] if not df.empty else 0} cols", output_folder, )
        except Exception:
            logger.exception("Error in _merge")
            raise

    def _generate_sql(output_folder: str):
        try:
            df = pd.read_csv(f"{output_folder}/merge.csv")
            logger.info(f"Generating inserts from CSV shape={df.shape}")
            with open(f"{output_folder}/inserts.sql", "w") as f:
                f.write(
                    "CREATE TABLE IF NOT EXISTS characters (\n"
                    "  race TEXT,\n"
                    "  name TEXT,\n"
                    "  proficiences TEXT,\n"
                    "  language TEXT,\n"
                    "  spells TEXT,\n"
                    "  class TEXT,\n"
                    "  levels TEXT,\n"
                    "  attributes TEXT\n"
                    ");\n"
                )
                for _, row in df.iterrows():
                    race = row.get("races", "")
                    name = row.get("names", "")
                    proficience = row.get("proficiences", "")
                    language = row.get("languages", "")
                    spells = row.get("spells", "")
                    class_ = row.get("classes", "")
                    levels = row.get("levels", "")
                    attributes_ = row.get("attributes", "")
                    f.write(
                        "INSERT INTO characters VALUES ("
                        f"'{race}', '{name}', $${proficience}$$, '{language}', $${spells}$$, '{class_}', '{levels}', $${attributes_}$$"
                        ");\n"
                    )

            logger.info(f"Wrote SQL file: {output_folder}/inserts.sql")
        except Exception:
            logger.exception("Error in _insert")
            raise

    # ----------  TASKS ----------
    name_race = PythonOperator(
        task_id = "name_race",
        python_callable = _name_race,
        op_kwargs = {
            "output_folder": output_folder,
            "endpoint": "races",
            "url": api_url
        }
    )

    attributes = PythonOperator(
        task_id = "attributes",
        python_callable = _attributes,
        op_kwargs = {
            "output_folder": output_folder,
            "endpoint": "attributes",
            "url": api_url
        }
    )

    languages = PythonOperator(
        task_id = "languages",
        python_callable = _languages,
        op_kwargs = {
            "output_folder": output_folder,
            "endpoint": "languages",
            "url": api_url
        }
    )

    classes = PythonOperator(
        task_id = "classes",
        python_callable = _classes,
        op_kwargs = {
            "output_folder": output_folder,
            "endpoint": "classes",
            "url": api_url
        }
    )

    proficiency_choices = PythonOperator(
        task_id = "proficiency_choices",
        python_callable = _proficiency_choices,
        op_kwargs = {
            "output_folder": output_folder,
            "endpoint": "proficiency_choices",
            "url": api_url
        }
    )

    levels = PythonOperator(
        task_id = "levels",
        python_callable = _levels,
        op_kwargs = {
            "output_folder": output_folder,
            "endpoint": "levels",
            "url": api_url
        }
    )

    spell_check = BranchPythonOperator(
        task_id = "spell_check",
        python_callable = _spell_check,
        op_kwargs = {
            "output_folder": output_folder,
            "endpoint": "spell_check",
            "url": api_url
        }
    )

    spells = PythonOperator(
        task_id = "spells",
        python_callable = _spells,
        op_kwargs = {
            "output_folder": output_folder,
            "endpoint": "spells",
            "url": api_url
        }
    )

    merge = PythonOperator(
        task_id = "merge",
        python_callable = _merge,
        op_kwargs = {
            "output_folder": output_folder,
            "endpoint": "merge",
            "url": api_url
        }
    )

    generate_sql = PythonOperator(
        task_id = "generate_sql",
        python_callable = _generate_sql,
        op_kwargs = {
            "output_folder": output_folder,
            "url": api_url
        }
    )

    insert_inserts = SQLExecuteQueryOperator(
        task_id="insert_inserts",
        conn_id="postgres_default",
        sql="inserts.sql",
        autocommit=True,
    )

    finale =  EmptyOperator(task_id="finale")

    # ---------- GRAPH ----------
    [name_race, attributes, languages, classes] >> proficiency_choices
    proficiency_choices >> levels >> spell_check >> [spells, merge]
    spells >> merge >> generate_sql >> insert_inserts >> finale