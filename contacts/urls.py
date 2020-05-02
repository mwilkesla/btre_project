from django.urls import path

from . import views

from .models import Contact

urlpatterns = [
    path('contact', views.contact, name='contact')
]