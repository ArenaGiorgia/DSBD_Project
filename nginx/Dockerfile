FROM nginx:latest

# Rimuovi la config di default
RUN rm -f /etc/nginx/conf.d/default.conf

# ECCO LA CORREZIONE: Aggiungi "configs/" prima del nome file
COPY configs/routing.conf /etc/nginx/conf.d/routing.conf