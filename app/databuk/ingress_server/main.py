# databulk/main.py

from flask import Flask
from weather import weather_bp
from sensors.endpoints import sensors_bp
from tree.endpoints import tree_bp

def create_app():
    app = Flask(__name__)
    app.register_blueprint(weather_bp, url_prefix="/api/v1/zarr-fuse/weather")
    app.register_blueprint(sensors_bp, url_prefix="/api/v1/zarr-fuse/sensors")
    app.register_blueprint(tree_bp, url_prefix="/api/v1/zarr-fuse/tree")
    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, port=8000)
