
from django.shortcuts import render, HttpResponse
from django.http import HttpResponse, JsonResponse
from django.template.loader import get_template
from django import template
from django.template import *
import json
import requests
from django.views.decorators.csrf import csrf_protect


host=''


def index(request):
    if "key_word" in request.POST :
        selected_option = request.POST["key_word"]
        print(selected_option)
        if selected_option :
            query = json.dumps({
                'size': 1000,
                "query": {
                    "match": {
                        "content": selected_option
                    }
                }
            })
            result = requests.get(host, data=query)
            results = json.loads(result.text)
            listofDicts = [dict() for num in range(len(results['hits']['hits']))]
            for idx, elements in enumerate(listofDicts):
                sourceValue = results['hits']['hits'][idx]['_source']
                tempCoordinates = str(sourceValue['coordinates']).strip("'").strip('[').strip(']').split(',')
                #print(sourceValue)
                listofDicts[idx] = dict(lng=float(tempCoordinates[0]), lat=float(tempCoordinates[1]))
            #print(listofDicts)
            return render(request,"twittapp/tweet_map.html",{"lats":listofDicts})

    else :
        selected_option = None
    return render(request, "twittapp/index.html", {'selected_option': selected_option})


def IndexPage(request):
    return render(request, "twittapp/tweet_map.html")
