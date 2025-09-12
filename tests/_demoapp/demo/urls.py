import logging

from django.contrib.admin.sites import site
from django.urls import path

logger = logging.getLogger(__name__)

urlpatterns = (path(r"", site.urls),)
