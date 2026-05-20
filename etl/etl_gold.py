import sys
import os

sys.path.append(os.path.abspath("./.secrets"))

import pandas as pd
import numpy as np
import datetime
import pickle

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score
)

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder

from psycopg2.extras import execute_values
from db_config import get_connection


# =============================================
# LOG
# =============================================
log_file = "etl.log"


def escribir_log(msg):

    with open(log_file, "a", encoding="utf-8") as f:

        f.write(
            f"{datetime.datetime.now()} - {msg}\n"
        )


def log_db(conn, pipeline, msg):

    try:

        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO meta.pipeline_log(
                pipeline_name,
                mensaje
            )
            VALUES (%s,%s)
        """, (pipeline, msg))

        conn.commit()

    except Exception as e:

        print("Error log_db:", e)


# =============================================
# LIMPIEZA TEXTO
# =============================================
def clean_text_columns(df):

    cols = [
        "producto",
        "categoria_producto",
        "tipo_promocion",
        "tipo_zona",
        "ubicacion_tienda",
        "clima",
        "dia_semana"
    ]

    for col in cols:

        if col in df.columns:

            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.lower()
            )

    return df


# =============================================
# FEATURE ENGINEERING AVANZADO
# =============================================
def feature_engineering(df):

    # =========================================
    # FECHA
    # =========================================
    df["fecha"] = pd.to_datetime(
        df["fecha"],
        errors="coerce"
    )

    # =========================================
    # HORA
    # =========================================
    df["hora"] = (
        pd.to_datetime(
            df["hora"],
            errors="coerce"
        )
        .dt.hour
        .fillna(0)
        .astype(int)
    )

    # =========================================
    # DÍA DEL MES
    # =========================================
    df["dia_mes"] = (
        df["fecha"]
        .dt.day
    )

    # =========================================
    # TRIMESTRE
    # =========================================
    df["trimestre"] = (
        df["fecha"]
        .dt.quarter
    )

    # =========================================
    # FIN DE SEMANA
    # =========================================
    df["es_fin_semana"] = (
        df["dia_semana"]
        .isin([
            "saturday",
            "sunday"
        ])
        .astype(int)
    )

    # =========================================
    # HORA PICO
    # =========================================
    df["hora_pico"] = df["hora"].apply(
        lambda x: 1 if (
            12 <= x <= 14 or
            18 <= x <= 21
        ) else 0
    )

    # =========================================
    # TEMPORADA
    # =========================================
    df["temporada"] = df["mes"].apply(
        lambda x:
            "Q1" if x <= 3 else
            "Q2" if x <= 6 else
            "Q3" if x <= 9 else
            "Q4"
    )

    # =========================================
    # INTERACCIÓN PRODUCTO + PROMO
    # =========================================
    df["producto_promocion"] = (
        df["producto"]
        + "_"
        + df["tipo_promocion"]
    )

    # =========================================
    # INTERACCIÓN TIENDA + CLIMA
    # =========================================
    df["zona_clima"] = (
        df["tipo_zona"]
        + "_"
        + df["clima"]
    )

    return df


# =============================================
# FEATURES
# =============================================
FEATURES = [

    # numéricas
    "mes",
    "dia_mes",
    "hora",
    "precio_unitario",
    "trimestre",
    "es_fin_semana",
    "hora_pico",

    # categóricas
    "producto",
    "categoria_producto",
    "tipo_promocion",
    "tipo_zona",
    "ubicacion_tienda",
    "clima",
    "dia_semana",
    "temporada",
    "producto_promocion",
    "zona_clima"
]


# =============================================
# TRAIN MODEL
# =============================================
def train_model(conn, pipeline_name):

    log_db(
        conn,
        pipeline_name,
        "INICIO TRAIN"
    )

    # =========================================
    # LOAD DATA
    # =========================================
    df = pd.read_sql(
        """
        SELECT *
        FROM gold_ml.ventas_dataset
        """,
        conn
    )

    if df.empty:

        raise Exception(
            "No hay datos para entrenar"
        )

    # =========================================
    # LIMPIEZA
    # =========================================
    df = clean_text_columns(df)

    # =========================================
    # FEATURE ENGINEERING
    # =========================================
    df = feature_engineering(
        df.copy()
    )

    # =========================================
    # TARGET
    # =========================================
    y = df["cantidad_vendida"]

    # =========================================
    # FEATURES
    # =========================================
    X = df[FEATURES]

    # =========================================
    # COLUMNAS
    # =========================================
    categorical = X.select_dtypes(
        include=["object", "string"]
    ).columns

    numeric = X.select_dtypes(
        exclude=["object", "string"]
    ).columns

    # =========================================
    # PREPROCESS
    # =========================================
    preprocess = ColumnTransformer(
        transformers=[

            (
                "cat",

                OneHotEncoder(
                    handle_unknown="ignore"
                ),

                categorical
            ),

            (
                "num",
                "passthrough",
                numeric
            )
        ]
    )

    # =========================================
    # RANDOM FOREST MEJORADO
    # =========================================
    model = RandomForestRegressor(

        n_estimators=700,

        max_depth=25,

        min_samples_split=5,

        min_samples_leaf=2,

        max_features="sqrt",

        bootstrap=True,

        random_state=42,

        n_jobs=-1
    )

    # =========================================
    # PIPELINE
    # =========================================
    pipeline = Pipeline([

        (
            "preprocess",
            preprocess
        ),

        (
            "model",
            model
        )
    ])

    # =========================================
    # SPLIT
    # =========================================
    X_train, X_test, y_train, y_test = (
        train_test_split(

            X,
            y,

            test_size=0.2,

            random_state=42,

            shuffle=True
        )
    )

    # =========================================
    # TRAIN
    # =========================================
    pipeline.fit(
        X_train,
        y_train
    )

    # =========================================
    # PREDICT TEST
    # =========================================
    preds = pipeline.predict(
        X_test
    )

    preds = np.round(preds)

    preds = np.clip(preds, 1, None)

    # =========================================
    # MÉTRICAS
    # =========================================
    mae = mean_absolute_error(
        y_test,
        preds
    )

    rmse = np.sqrt(
        mean_squared_error(
            y_test,
            preds
        )
    )

    r2 = r2_score(
        y_test,
        preds
    )

    # =========================================
    # PRINT MÉTRICAS
    # =========================================
    print("\n========================")
    print("MÉTRICAS MODELO")
    print("========================")

    print(f"MAE  : {mae:.4f}")
    print(f"RMSE : {rmse:.4f}")
    print(f"R2   : {r2:.4f}")

    # =========================================
    # LOG MÉTRICAS
    # =========================================
    log_db(
        conn,
        pipeline_name,
        f"MAE: {mae}"
    )

    log_db(
        conn,
        pipeline_name,
        f"RMSE: {rmse}"
    )

    log_db(
        conn,
        pipeline_name,
        f"R2: {r2}"
    )

    # =========================================
    # FEATURE IMPORTANCE
    # =========================================
    try:

        feature_names = (
            pipeline
            .named_steps["preprocess"]
            .get_feature_names_out()
        )

        importances = (
            pipeline
            .named_steps["model"]
            .feature_importances_
        )

        importance_df = pd.DataFrame({

            "feature": feature_names,

            "importance": importances

        }).sort_values(
            by="importance",
            ascending=False
        )

        print("\n========================")
        print("TOP VARIABLES")
        print("========================")

        print(
            importance_df.head(15)
        )

    except Exception as e:

        print(
            "Error feature importance:",
            e
        )

    # =========================================
    # GUARDAR MODELO
    # =========================================
    os.makedirs(
        "models",
        exist_ok=True
    )

    with open(
        "models/model.pkl",
        "wb"
    ) as f:

        pickle.dump(
            pipeline,
            f
        )

    with open(
        "models/features.pkl",
        "wb"
    ) as f:

        pickle.dump(
            FEATURES,
            f
        )

    # =========================================
    # LOG
    # =========================================
    log_db(
        conn,
        pipeline_name,
        "MODELO GUARDADO"
    )

    return mae, rmse, r2


# =============================================
# PREDICT
# =============================================
def predict_model(conn, pipeline_name):

    log_db(
        conn,
        pipeline_name,
        "INICIO PREDICT"
    )

    # =========================================
    # LOAD MODEL
    # =========================================
    with open(
        "models/model.pkl",
        "rb"
    ) as f:

        pipeline = pickle.load(f)

    with open(
        "models/features.pkl",
        "rb"
    ) as f:

        features = pickle.load(f)

    # =========================================
    # DATA
    # =========================================
    df_pred = pd.read_sql(
        """
        SELECT *
        FROM gold_ml.ventas_prediccion
        """,
        conn
    )

    if df_pred.empty:

        raise Exception(
            "No hay datos para predicción"
        )

    # =========================================
    # LIMPIEZA
    # =========================================
    df_pred = clean_text_columns(
        df_pred
    )

    # =========================================
    # FEATURE ENGINEERING
    # =========================================
    df_pred = feature_engineering(
        df_pred.copy()
    )

    # =========================================
    # FEATURES
    # =========================================
    X_new = df_pred[features]

    # =========================================
    # PREDICT
    # =========================================
    predicciones = pipeline.predict(
        X_new
    )

    predicciones = np.round(
        predicciones
    )

    predicciones = np.clip(
        predicciones,
        1,
        None
    )

    df_pred["cantidad_predicha"] = (
        predicciones.astype(int)
    )

    # =========================================
    # HORA
    # =========================================
    df_pred["hora"] = (
        pd.to_datetime(
            df_pred["hora"],
            format="%H",
            errors="coerce"
        )
        .dt.time
    )

    # =========================================
    # INSERT DB
    # =========================================
    cursor = conn.cursor()

    cursor.execute("""
        TRUNCATE TABLE
        gold_ml.ventas_predicha
        RESTART IDENTITY
    """)

    conn.commit()

    records = df_pred[[

        "fecha",
        "mes",
        "dia_semana",
        "hora",
        "ubicacion_tienda",
        "tipo_zona",
        "producto",
        "categoria_producto",
        "precio_unitario",
        "tipo_promocion",
        "clima",
        "cantidad_predicha"

    ]].values.tolist()

    execute_values(cursor, """

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

        VALUES %s

    """, records)

    conn.commit()

    log_db(
        conn,
        pipeline_name,
        "PREDICCIONES GUARDADAS"
    )


# =============================================
# MAIN
# =============================================
def main():

    pipeline_name = "etl_gold_ml"

    escribir_log(
        "INICIO PIPELINE GOLD ML"
    )

    conn = get_connection()

    try:

        mae, rmse, r2 = train_model(
            conn,
            pipeline_name
        )

        predict_model(
            conn,
            pipeline_name
        )

        print("\n🚀 PIPELINE ML COMPLETO")

    except Exception as e:

        print("ERROR:", e)

        log_db(
            conn,
            pipeline_name,
            f"ERROR: {e}"
        )

    finally:

        conn.close()


if __name__ == "__main__":

    main()
