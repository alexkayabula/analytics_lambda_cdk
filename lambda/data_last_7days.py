import json
import logging
import requests


GOOGLE_ANALYTICS_URL = "https://analyticsdata.googleapis.com/v1beta/properties/327357256:runReport?access_token="

# Fetch most visited pages in the last 7 days data.
def fetch_visited_pages(access_token):
    url = f'{GOOGLE_ANALYTICS_URL}{access_token}'
    try:
        logging.info("[Google Analytics] Fetching most visited pages data.")
        request_body = {"dimensions":[{"name":"unifiedScreenName"}],
                                        "metrics":[{"name":"screenPageViews"},
                                        {"name":"totalUsers"},
                                        {"name":"userEngagementDuration"}],
                                        "dateRanges":[{"startDate":"7daysAgo","endDate":"today"}]}
        response = requests.post(url, json=request_body)
    except Exception as e:
        logging.debug("[Google Analytics] Error fetching data", e)
    else:
        result = json.loads(response.text)
        print(result)
        data = result["rows"]
        most_visited_pages = []
        for item in data:
            views = item["metricValues"][0]["value"]
            users = item["metricValues"][1]['value']
            pages = item["dimensionValues"][0]['value']
            if float(users) != 0.0:
                views_per_user = float(views) / float(users)
                total_engagement_time = item["metricValues"][2]["value"]
                avg_engagement_time = float(total_engagement_time)/ float(users)

                most_visited_pages.append({"page": pages, 
                                                                        "views": views,
                                                                        "users" : users,
                                                                        "views_per_user": views_per_user,
                                                                        "average_engagement_time": avg_engagement_time})
            else:
                views_per_user = 'n/a'
                avg_engagement_time = 'n/a'

                most_visited_pages.append({"page": pages, 
                                                                        "views": views,
                                                                        "users" : users,
                                                                        "views_per_user": views_per_user,
                                                                        "average_engagement_time": avg_engagement_time})
    
        return most_visited_pages
