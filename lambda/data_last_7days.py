import os
import json
import logging
import requests


GOOGLE_ANALYTICS_URL = os.getenv('GOOGLE_ANALYTICS_URL')

# Fetch most visited pages in the last 7 days data.
def fetch_visited_pages(access_token):
    url = f'{GOOGLE_ANALYTICS_URL}{access_token}'
    try:
        logging.info("[Google Analytics] Fetching user mos visited pages data.")
        request_body = {"dimensions": [{"name": "unifiedPagePathScreen"}, 
                                       {"name": "unifiedScreenName"}],  "metrics": [{"name": "averageSessionDuration"}, 
                                      {"name": "screenPageViews"}, {"name": "screenPageViewsPerSession"}, 
                                      {"name": "totalUsers"}], "dateRanges": [{"startDate": "7daysAgo", "endDate": "today"}]}
        response = requests.post(url, json=request_body)
    except Exception as e:
        logging.debug("[Google Analytics] Error fetching data", e)
    else:
        result = json.loads(response.text)
        data = result["rows"]
        most_visited_pages = []
        for item in data:
            views = item["metricValues"][0]["value"]
            users = item["metricValues"][1]['value']
            if float(users) != 0.0:
                views_per_user = float(views) / float(users)
                total_engagement_time = item["metricValues"][2]["value"]
                avg_engagement_time = float(total_engagement_time)/ float(users)

                most_visited_pages.append({"page": item["dimensionValues"][1]['value'], 
                                                                        "views": item["metricValues"][0]["value"],
                                                                         "users" : users,
                                                                        "info": item["dimensionValues"][0]['value'],
                                                                        "views_per_user": views_per_user,
                                                                        "average_engagement_time": avg_engagement_time})
            else:
                views_per_user = 'n/a'
                avg_engagement_time = 'n/a'

                most_visited_pages.append({"page": item["dimensionValues"][1]['value'], 
                                                                        "views": item["metricValues"][0]["value"],
                                                                         "users" : users,
                                                                        "info": item["dimensionValues"][0]['value'],
                                                                        "views_per_user": views_per_user,
                                                                        "average_engagement_time": avg_engagement_time})
    
        return most_visited_pages
