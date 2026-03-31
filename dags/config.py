import os

base_env = {
    "MINIO_ENDPOINT":   os.getenv("MINIO_ENDPOINT"),
    "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY"),
    "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY"),
}

silver_env = {
    **base_env,
    "BRONZE_PATH":     os.getenv("BRONZE_PATH"),
    "SILVER_PATH":     os.getenv("SILVER_PATH"),
    "VPD_PATH":        os.getenv("VPD_PATH"),
    "CHECKPOINT_PATH": os.getenv("CHECKPOINT_PATH"),
}

gold_env = {
    **base_env,
    "SILVER_PATH":   os.getenv("SILVER_PATH"),
    "VPD_PATH":      os.getenv("VPD_PATH"),
    "GOLD_PATH":     os.getenv("GOLD_PATH"),
    "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
    "POSTGRES_DB":   os.getenv("POSTGRES_DB"),
    "POSTGRES_USER": os.getenv("POSTGRES_USER"),
    "POSTGRES_PASS": os.getenv("POSTGRES_PASS"),
}