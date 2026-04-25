import pandas as pd
import requests
import datetime
from db_config import get_connection




# =============================
# LOG EN META
# =============================
def log_db(cursor, pipeline, msg):
    cursor.execute(
        """
        insert into meta.pipeline_log (pipeline_name, mensaje)
        values (%s, %s)
        """,
        (pipeline, msg)
    )


# =============================
# DESCARGAR DRIVE
# =============================
def descargar_drive(file_id):
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("Error descargando archivo")

    with open("data.csv", "wb") as f:
        f.write(response.content)

    return "data.csv"


# =============================
# MAIN
# =============================
def main():

    pipeline_name = "etl_bronze_datanueva"
    file_id = "11eJ2wOSARqqzSTa1nueyurvUj_6MVTZL"

    conn = get_connection()
    cursor = conn.cursor()

    run_id = None

    try:
        # =============================
        # INICIO PIPELINE
        # =============================
        cursor.execute(
            """
            insert into meta.pipeline_run (pipeline_name, fecha_inicio, estado)
            values (%s, now(), 'RUNNING')
            returning id
            """,
            (pipeline_name,)
        )
        run_id = cursor.fetchone()[0]

        log_db(cursor, pipeline_name, "Inicio pipeline")
        conn.commit()

        # =============================
        # DESCARGAR + LEER
        # =============================
        archivo = descargar_drive(file_id)
        df = pd.read_csv(archivo)
        df.columns = df.columns.str.strip()

        log_db(cursor, pipeline_name, f"Filas leídas: {len(df)}")

        # =============================
        # LIMPIEZA BÁSICA
        # =============================
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        df["mes"] = pd.to_numeric(df["mes"], errors="coerce")
        df["hora"] = pd.to_datetime(df["hora"], errors="coerce").dt.strftime('%H:%M:%S')
        df["precio_unitario"] = pd.to_numeric(df["precio_unitario"], errors="coerce")

        df["ingestion_timestamp"] = datetime.datetime.now()

        # =============================
        # TRUNCATE + COPY
        # =============================
        cursor.execute("TRUNCATE TABLE bronze.datanueva")
        log_db(cursor, pipeline_name, "TRUNCATE OK")

        temp_csv = "temp.csv"
        df.to_csv(temp_csv, index=False)

        with open(temp_csv, "r", encoding="utf-8") as f:
            cursor.copy_expert(
                """
                COPY bronze.datanueva(
                    transaction_id,
                    store_id,
                    fecha,
                    mes,
                    dia_semana,
                    hora,
                    ubicacion_tienda,
                    tipo_zona,
                    producto,
                    categoria_producto,
                    precio_unitario,
                    tipo_promocion,
                    clima,
                    ingestion_timestamp
                )
                FROM STDIN
                WITH CSV HEADER
                """,
                f
            )

        conn.commit()
        log_db(cursor, pipeline_name, "COPY OK")

        # =============================
        # FIN OK
        # =============================
        cursor.execute(
            """
            update meta.pipeline_run
            set fecha_fin = now(),
                estado = 'OK',
                filas_procesadas = %s
            where id = %s
            """,
            (len(df), run_id)
        )

        conn.commit()

        print("✅ Pipeline ejecutado correctamente")

    except Exception as e:

        conn.rollback()
        log_db(cursor, pipeline_name, f"ERROR: {e}")

        if run_id:
            cursor.execute(
                """
                update meta.pipeline_run
                set fecha_fin = now(),
                    estado = 'ERROR'
                where id = %s
                """,
                (run_id,)
            )
            conn.commit()

        print("❌ Error:", e)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()