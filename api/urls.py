from django.urls import path
from .views import download_and_process_view  # Ensure this view is correctly imported

urlpatterns = [
    path('trigger-download/', download_and_process_view, name='trigger_download'),  # Define the endpoint
    # Other URLs...
]
