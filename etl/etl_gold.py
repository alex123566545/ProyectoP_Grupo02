import os
import sys
import pandas as pd
import numpy as np
import datetime
import pickle

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# =============================
# CONEXIÓN
# =============================
sys.path.append(os.path.abspath("../.secrets"))
from db_config import get_connection


# =============================
# LOG TXT
# =============================
log_file = "etl.log"

def escribir_log(msg):
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{datetime.datetime.now()} - {msg}\n")


# =============================
# LOG DB
# =============================
def log_db(conn, pipeline, msg):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO meta.pipeline_log(pipeline_name, mensaje)
            VALUES (%s,%s)
        """, (pipeline, msg))
        conn.commit()
    except Exception as e:
        print("Error log_db:", e)


# =============================
# TRAIN MODEL
# =============================
def train_model(conn, pipeline_name):

    log_db(conn, pipeline_name, "INICIO TRAIN")

    df = pd.read_sql("""
        SELECT *
        FROM gold_ml.ventas_dataset
    """, conn)

    y = df["cantidad_vendida"]
    X = df.drop(columns=["cantidad_vendida"])

    X = pd.get_dummies(X)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestRegressor(
        n_estimators=150,
        random_state=42,
        n_jobs=-1
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    # =============================
    # MÉTRICAS
    # =============================
    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    r2 = r2_score(y_test, preds)

    print(f"MAE: {mae}")
    print(f"RMSE: {rmse}")
    print(f"R2: {r2}")

    log_db(conn, pipeline_name, f"MAE: {mae}")
    log_db(conn, pipeline_name, f"RMSE: {rmse}")
    log_db(conn, pipeline_name, f"R2: {r2}")

    # =============================
    # VALIDACIÓN
    # =============================
    if mae > 10:
        raise Exception("Modelo malo (MAE alto)")

    if r2 < 0.5:
        raise Exception("Modelo malo (R2 bajo)")

    # =============================
    # GUARDAR MODELO
    # =============================
    os.makedirs("models", exist_ok=True)

    with open("models/model.pkl", "wb") as f:
        pickle.dump(model, f)

    with open("models/columns.pkl", "wb") as f:
        pickle.dump(X.columns, f)

    log_db(conn, pipeline_name, "MODELO GUARDADO")

    return mae, rmse, r2


# =============================
# PREDICT MODEL
# =============================
def predict_model(conn, pipeline_name):

    log_db(conn, pipeline_name, "INICIO PREDICT")

    with open("models/model.pkl", "rb") as f:
        model = pickle.load(f)

    with open("models/columns.pkl", "rb") as f:
        cols = pickle.load(f)

    df_pred = pd.read_sql("""
        SELECT *
        FROM gold_ml.ventas_prediccion
    """, conn)

    X_new = pd.get_dummies(df_pred)
    X_new = X_new.reindex(columns=cols, fill_value=0)

    df_pred["cantidad_predicha"] = model.predict(X_new)

    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE gold_ml.ventas_predicha")
    conn.commit()

    for _, row in df_pred.iterrows():
        cursor.execute("""
            INSERT INTO gold_ml.ventas_predicha (
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
                cantidad_predicha
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row["fecha"],
            row["mes"],
            row["dia_semana"],
            row["hora"],
            row["ubicacion_tienda"],
            row["tipo_zona"],
            row["producto"],
            row["categoria_producto"],
            row["precio_unitario"],
            row["tipo_promocion"],
            row["clima"],
            row["cantidad_predicha"]
        ))

    conn.commit()

    log_db(conn, pipeline_name, "PREDICCIONES GUARDADAS")


# =============================
# MAIN (PIPELINE CONTROL)
# =============================
def main():

    pipeline_name = "etl_gold_ml"

    escribir_log("INICIO PIPELINE GOLD ML")

    conn = get_connection()
    cursor = conn.cursor()

    run_id = None

    try:

        # =============================
        # INICIO PIPELINE
        # =============================
        cursor.execute("""
            INSERT INTO meta.pipeline_run
            (pipeline_name, fecha_inicio, estado)
            VALUES (%s, now(), %s)
            RETURNING id
        """, (pipeline_name, "RUNNING"))

        run_id = cursor.fetchone()[0]
        conn.commit()

        log_db(conn, pipeline_name, "PIPELINE START")

        # =============================
        # TRAIN + PREDICT
        # =============================
        mae, rmse, r2 = train_model(conn, pipeline_name)
        predict_model(conn, pipeline_name)

        # =============================
        # FIN OK
        # =============================
        cursor.execute("""
            UPDATE meta.pipeline_run
            SET fecha_fin = now(),
                estado = %s,
                filas_procesadas = (
                    SELECT COUNT(*) FROM gold_ml.ventas_predicha
                )
            WHERE id = %s
        """, ("OK", run_id))

        conn.commit()

        log_db(conn, pipeline_name, f"PIPELINE OK | MAE={mae} RMSE={rmse} R2={r2}")

        print("🚀 PIPELINE ML COMPLETO")

    except Exception as e:

        conn.rollback()

        print("ERROR:", e)
        log_db(conn, pipeline_name, f"ERROR {e}")

        if run_id:
            cursor.execute("""
                UPDATE meta.pipeline_run
                SET fecha_fin = now(),
                    estado = 'ERROR'
                WHERE id = %s
            """, (run_id,))
            conn.commit()

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()