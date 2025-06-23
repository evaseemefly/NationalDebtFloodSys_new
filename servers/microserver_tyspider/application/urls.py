# from controller import *
from controller.typhoon import app as typhoon_app

urlpatterns = [
    {"ApiRouter": typhoon_app, "prefix": "/typhoon", "tags": ["spider typhoon"]},
]
