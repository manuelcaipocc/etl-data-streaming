import docker
import threading
import solace.messaging.messaging_service as messaging_service
from solace.messaging.resources.topic import Topic
from solace.messaging.receiver.direct_message_receiver import DirectMessageReceiver
from solace.messaging.config.retry_strategy import RetryStrategy
from dagster import sensor, RunRequest, Definitions

# Configuración de Solace
SOLACE_HOST = "solace-broker"
SOLACE_PORT = 1883  # Asegúrate de que este sea el puerto correcto
SOLACE_USERNAME = "Boschrexroth"
SOLACE_PASSWORD = "Boschrexroth"
SOLACE_VPN = "default"
TOPIC = "etl/transform"

# Contenedor a reiniciar
ETL_TRANSFORM_CONTAINER = "etl-transform"

# Cliente de Docker
docker_client = docker.from_env()
event_detected = False

def on_message(message):
    """Callback cuando se recibe un mensaje en Solace."""
    global event_detected
    event_detected = True  # Marca que se recibió un evento

    try:
        container = docker_client.containers.get(ETL_TRANSFORM_CONTAINER)
        container.restart()
        print(f"Contenedor {ETL_TRANSFORM_CONTAINER} reiniciado correctamente.")
    except Exception as e:
        print(f"Error al reiniciar {ETL_TRANSFORM_CONTAINER}: {e}")

def solace_listener():
    """Hilo separado para escuchar mensajes de Solace utilizando Solace PubSub+ SDK."""
    global event_detected

    # Configuración del servicio de mensajería
    broker_props = {
        "solace.messaging.transport.host": f"tcp://{SOLACE_HOST}:{SOLACE_PORT}",
        "solace.messaging.service.vpn-name": SOLACE_VPN,
        "solace.messaging.authentication.scheme.basic.username": SOLACE_USERNAME,
        "solace.messaging.authentication.scheme.basic.password": SOLACE_PASSWORD
    }

    # Crear servicio de mensajería
    service = messaging_service.MessagingService.builder().from_properties(broker_props).build()
    service.connect()
    print("Conectado a Solace PubSub+")

    # Crear receptor de mensajes directos
    receiver = service.create_direct_message_receiver_builder() \
        .with_subscription(Topic.of(TOPIC)) \
        .with_retry_strategy(RetryStrategy.CONNECT_RETRY) \
        .build()
    
    receiver.start()
    print(f"Escuchando en el topic: {TOPIC}")

    # Callback cuando llega un mensaje
    def message_handler(incoming_message):
        print(f"Mensaje recibido en {TOPIC}: {incoming_message.get_payload_as_string()}")
        on_message(incoming_message)

    receiver.receive_async(message_handler)

    # Mantener el hilo corriendo
    while True:
        pass  # Se podría mejorar con un mecanismo de cierre limpio

# Inicia el listener en un hilo separado
threading.Thread(target=solace_listener, daemon=True).start()

@sensor()
def solace_transform_sensor(context):
    """Sensor de Dagster para procesar eventos de transformación."""
    global event_detected
    if event_detected:
        event_detected = False  # Resetear flag
        return RunRequest(run_key=None)
    return None  # No hay evento nuevo

# 🔹 Registrar el sensor en Dagster
defs = Definitions(sensors=[solace_transform_sensor])
