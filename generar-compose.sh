#!/bin/bash

echo "Nombre del archivo de salida: $1"
echo "Cantidad de clientes: $2"
python3 docker_compose_generator/compose_generator.py $1 $2