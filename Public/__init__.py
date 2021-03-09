import logging

import azure.functions as func
import requests

def main(req: func.HttpRequest) -> func.HttpResponse:
    URL = 'http://20.194.0.206'
    responese = requests.get(URL)
    return func.HttpResponse(responese.text)
