import validators

class UrlValidator:

    def validate_url_contains_zip_file(url): 
        return validators.url(url)
        