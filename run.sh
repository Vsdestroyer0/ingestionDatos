#!/bin/bash

# Base directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PYTHON="$DIR/.venv/bin/python -u"
VENV_STREAMLIT="$DIR/.venv/bin/streamlit"

# Helper print functions
print_blue() {
    echo -e "\e[34m$1\e[0m"
}
print_green() {
    echo -e "\e[32m$1\e[0m"
}
print_red() {
    echo -e "\e[31m$1\e[0m"
}

show_usage() {
    echo "Uso: $0 [opcion]"
    echo ""
    echo "Opciones:"
    echo "  init      Inicializa el esquema de la base de datos (crea tablas)"
    echo "  producer  Arranca el productor de eventos Kafka (ingesta Steam API)"
    echo "  consumer  Arranca el consumidor de eventos Kafka (carga a Postgres)"
    echo "  dashboard Arranca el panel analitico interactivo de Streamlit"
    echo "  services  Inicia los contenedores de Docker (Postgres y Kafka)"
    echo "  help      Muestra esta ayuda"
}

case "$1" in
    init)
        print_blue "Inicializando base de datos PostgreSQL..."
        $VENV_PYTHON "$DIR/init_db.py"
        ;;
    producer)
        print_blue "Arrancando productor de eventos Kafka (Steam API)..."
        $VENV_PYTHON "$DIR/producer.py"
        ;;
    consumer)
        print_blue "Arrancando consumidor de eventos Kafka (Procesamiento y Carga)..."
        $VENV_PYTHON "$DIR/consumer.py"
        ;;
    dashboard)
        print_blue "Arrancando Dashboard de Streamlit..."
        $VENV_STREAMLIT run "$DIR/dashboard.py"
        ;;
    services)
        print_blue "Iniciando contenedores de Postgres y Kafka en el host..."
        distrobox-host-exec podman-compose up -d
        ;;
    *)
        show_usage
        exit 1
        ;;
esac
