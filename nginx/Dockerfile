FROM nginx:1.25-alpine

# Remove default config
RUN rm /etc/nginx/conf.d/default.conf

# Copy custom nginx config
COPY nginx.conf /etc/nginx/nginx.conf

# Add wget for health checks
RUN apk add --no-cache wget

EXPOSE 80 8080

CMD ["nginx", "-g", "daemon off;"]