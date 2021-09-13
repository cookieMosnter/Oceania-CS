from django import template
from django.shortcuts import render
from django.http import HttpResponse
from django.template import loader
from .models import *
from .api import *
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect

from ringcentral import SDK
import urllib

RINGCENTRAL_CLIENT_ID = 'd-XQpUS0S1KgB6BHn67i5g'
RINGCENTRAL_CLIENT_SECRET = 'SqZF-zJkSWeCLqE_4nMMzAa0cz6Pr1SbGEcO56b3S5sA'
RINGCENTRAL_SERVER_URL = 'https://platform.devtest.ringcentral.com'
RINGCENTRAL_REDIRECT_URL = 'http://127.0.0.1:8000/kpiGen/index'

rcsdk = SDK(RINGCENTRAL_CLIENT_ID, RINGCENTRAL_CLIENT_SECRET, RINGCENTRAL_SERVER_URL)




# Create your views here.
@login_required(login_url="/login/")
def report(request):

    de = DataExtractor(User.objects.all(), Department.objects.all(), TimeTable.objects.all()[0])
    context = {
        'columnNames': de.INDIVIDUALCOLUMNNAMES,
        'dataList': de.getIndividualList(),

        'departmentNames': de.DEPARTMENTCOLUMNNAMES,
        'departmentDataList': de.getDepartmentList()
    }
    try:

        load_template = request.path.split('/')[-1]
        context['segment'] = load_template

        html_template = loader.get_template(load_template)
        return HttpResponse(html_template.render(context, request))

    except template.TemplateDoesNotExist:

        html_template = loader.get_template('page-404.html')
        return HttpResponse(html_template.render(context, request))

    except:

        html_template = loader.get_template('page-500.html')
        return HttpResponse(html_template.render(context, request))


def index(request):
    return redirect('/reports.html')


def login(request):
    base_url = RINGCENTRAL_SERVER_URL + '/restapi/oauth/authorize'
    params = (
        ('response_type', 'code'),
        ('redirect_uri', RINGCENTRAL_REDIRECT_URL),
        ('client_id', RINGCENTRAL_CLIENT_ID),
        ('state', 'initialState')
    )
    auth_url = base_url + '?' + urllib.parse.urlencode(params)

    context = {
        'authorize_uri': auth_url
    }
    return render(request, 'kpiGen/login.html', context)
