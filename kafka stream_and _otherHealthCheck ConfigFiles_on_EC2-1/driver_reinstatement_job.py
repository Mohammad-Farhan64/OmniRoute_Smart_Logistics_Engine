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
# AUTO REINSTATEMENT ENGINE
# ---------------------------------------------------

cursor.execute(
    """
    UPDATE driver_risk_state

    SET
        suspension_status = 'ACTIVE',
        suspension_reason = NULL,
        reinstatement_eligible = 1,
        updated_ts = CURRENT_TIMESTAMP

    WHERE
        cooldown_until IS NOT NULL
        AND cooldown_until <= CURRENT_TIMESTAMP
        AND suspension_status != 'ACTIVE'
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

        'AUTO_REINSTATEMENT',

        suspension_status,

        'ACTIVE',

        total_strike_score,

        'Cooldown expired'

    FROM driver_risk_state

    WHERE
        reinstatement_eligible = 1
    """
)

# ---------------------------------------------------
# COMMIT
# ---------------------------------------------------

conn.commit()

cursor.close()

conn.close()

print(
    "Driver reinstatement engine completed successfully."
)