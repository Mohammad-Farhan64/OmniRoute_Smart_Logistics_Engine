import config
import psycopg2

# ---------------------------------------------------
# POSTGRES CONNECTION
# ---------------------------------------------------

conn = psycopg2.connect(

    host=config.POSTGRES_HOST,

    database=config.POSTGRES_DB,

    user=config.POSTGRES_USER,

    password=config.POSTGRES_PASSWORD,

    port=config.POSTGRES_PORT
)

cursor = conn.cursor()

# ---------------------------------------------------
# STRIKE DECAY ENGINE
# ---------------------------------------------------

cursor.execute(
    """
    UPDATE driver_risk_state

    SET

        total_strike_score =

            GREATEST(
                total_strike_score - 1,
                0
            ),

        strike_decay_score =

            GREATEST(
                strike_decay_score - 1,
                0
            ),

        updated_ts = CURRENT_TIMESTAMP
    """
)

# ---------------------------------------------------
# AUDIT HISTORY
# ---------------------------------------------------

cursor.execute(
    """
    INSERT INTO suspension_event_history (

        driver_id,
        event_type,
        previous_status,
        new_status,
        strike_score,
        event_reason

    )

    SELECT

        driver_id,

        'STRIKE_DECAY',

        suspension_status,

        suspension_status,

        total_strike_score,

        'Automated strike decay applied'

    FROM driver_risk_state
    """
)

# ---------------------------------------------------
# COMMIT
# ---------------------------------------------------

conn.commit()

cursor.close()

conn.close()

print(
    "Driver strike decay engine completed successfully."
)