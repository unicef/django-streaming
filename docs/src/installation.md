# Installation

To install `django-streaming`, follow these steps:

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/UNICEF/django-streaming.git
    cd django-streaming
    ```

2.  **Install dependencies:**

    ```bash
    pip install -e .
    ```

3.  **Set up your Django project:**

    Add `streaming.apps.StreamingConfig` to your `INSTALLED_APPS` in your Django project's `settings.py`:

    ```python
    INSTALLED_APPS = [
        # ... other apps
        "streaming.apps.StreamingConfig",
    ]
    ```
