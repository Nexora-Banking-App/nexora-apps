FROM nginx:alpine

# Copy our custom HTML
COPY index.html /usr/share/nginx/html/index.html

# Expose Nginx port
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]