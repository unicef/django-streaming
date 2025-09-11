from django.contrib.admin.sites import site
from django.urls import path

urlpatterns = (path(r"", site.urls),)
