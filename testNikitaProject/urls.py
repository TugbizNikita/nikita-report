"""CGReport URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from .views import index, wpr_report, download_consolidated_report, wsr_report, download_wsr, download_attendance, download_wpr_report, validate_wsr, validate_consolidated_sheet, validate_candidates, wpr_stats_comm, attendance_stats, student_all_data, validate_wsr, validate_wpr,leadership, validate_consolidated, gamification_registration1, gamification_login1,read_sheet_wrapper
urlpatterns = [
    path('admin/', admin.site.urls),
    path('', index),
    path('wpr/', wpr_report),
    path('download_c/', download_consolidated_report),
    path('wsr/', wsr_report),
    path('download_wsr/', download_wsr),
    path('download_attendance/', download_attendance),
    path('download_wpr/', download_wpr_report),
    path('validation_wsr/', validate_wsr),
    path('validation_wpr/', validate_wpr),
    #path('validate_consolidated/', validate_consolidated),
    path('validate_candidates/', validate_candidates),
    path('wpr_stats/', wpr_stats_comm),
    path('attendance_stats/', attendance_stats),
    path('student_all_data/', student_all_data),
    #path('validateWSR/', validate_wsr),
    path('validateConsolidated/',validate_consolidated_sheet),
    path('leadership/',leadership),
    path('gamification_registration/', gamification_registration1),
    path('gamification_login/', gamification_login1),
    path('read_sheet/', read_sheet_wrapper),
    
    
]
