import json
import logging
import requests
from date_formatter import formatDate


GOOGLE_ANALYTICS_URL = "https://analyticsdata.googleapis.com/v1beta/properties/327357256:runReport?access_token="

# Fetch most visited pages in the last 365 days data.
def fetch_visited_pages_365days(access_token):
    url = f'{GOOGLE_ANALYTICS_URL}{access_token}'
    try:
        logging.info("[Google Analytics] Fetching most visited pages data for last 365 days.")
        request_body = {"dimensions":[{"name":"date"},{"name":"unifiedScreenName"}],
                                        "metrics":[{"name":"screenPageViews"},
                                        {"name":"totalUsers"},
                                        {"name":"userEngagementDuration"}],
                                        "dateRanges":[{"startDate":"365daysAgo","endDate":"today"}]}
        response = requests.post(url, json=request_body)
    except Exception as e:
        logging.debug("[Google Analytics] Error fetching data", e)
    else:
        result = json.loads(response.text)
        data = result["rows"]
        most_visited_pages = []
        for item in data:
            date_string = item["dimensionValues"][0]['value']
            date = formatDate(date_string)
            views = item["metricValues"][0]["value"]
            users = item["metricValues"][1]['value']
            pages = item["dimensionValues"][1]['value']
            if float(users) != 0.0:
                views_per_user = float(views) / float(users)
                total_engagement_time = item["metricValues"][2]["value"]
                avg_engagement_time = float(total_engagement_time)/ float(users)

                most_visited_pages.append({"date": date,"page": pages, 
                                                                        "views": views,
                                                                        "users" : users,
                                                                        "views_per_user": views_per_user,
                                                                        "average_engagement_time": avg_engagement_time})
            else:
                views_per_user = 'n/a'
                avg_engagement_time = 'n/a'

                most_visited_pages.append({"date": date,"page": pages, 
                                                                        "views": views,
                                                                        "users" : users,
                                                                        "views_per_user": views_per_user,
                                                                        "average_engagement_time": avg_engagement_time})
    
        return most_visited_pages
fetch_visited_pages_365days('ya29.c.b0Aaekm1JyOWJMW7o-H2U6O7r-8yFEP3u3ZO6N6caVejEp0DdthKo5VRX1MmFM0Ru7FzLW7FddGQdZkXxRSB3dlthE6dXu-6P_K9yM-DSU0rZvqhRHNclXTZV9b0PfpmwuAxoje6unrLht0UuQJR1_maCfyLmGWE0_CDCl64sEExKEmzoB4-KWxT5xTXqFv9dG0kOE3ODidQl7sbNOV240hVr-NGVOi9kphHEeAeknsc6g37ATzymuXFEVuvNfltsurHxl2ewh9Wxosexlude81ZaG6S74Xum408oTBkLdVSYOByeggRXicrJe8VyIDajxK8nZcskG336A2IOpOJ0_xwwd56YhM8rYF2mijfYd37fbtBrdZxsvn4ybYrM40gVrhMsy9B7ygS0qMqns4e7oM0Z0lBirl2Mjk1koUfSOUY0Vl0-WBW2QmJOgvfO_f0vkdbxR3j_mw0s_SSr8lrrxzx806Rhg-1f2uz1BhhbR_QOh0opMjsZWJB0vYWb_tX6Os8gw5oQRxIdr9ZRoVzavs5fWewuR5lbzu_4oRQI6MQ_jzrlsJi8BJX9u9vhrzSFhQ175U_tdxlQqyMbsQY2jg4tMrS1hRb3YX69FfV4R5ifJQ1OJ8J1uROa8kmWglb3ct2rfzSbZ7hjt426-Rwag8WtuoWjWQRxtBI0wewr0cglcg6Ry6zB0ufQ0_f7o4nuyIWX9nJwpOZizQ9yiklmXIqO3k_WpeYRw1I0hy1oJap47ZiQRxyqaMgOag44ccXM7I03-yp_roJc20IbtWtY1cO1y7BQcXQO4S-q3mniIcn5xZ4d788JsVz4B9Fw_iBa857twbp2sosvzchtdjZ7jxqn4wzbnOVXF6q69FrraIslgouy50UB6ewJZnlOwjnwVRuY4bJqwyqba30Y5lpVWR71dy6-dhry1fu7dgcyul3J9r-kiBzr452M2Xz1YaUv4So9w_Qx5nhFeIn-zY7-WR_Rv4cdvIppUpq4Uug4u39OdggaMv_tYY3p')