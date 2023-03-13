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
        print(most_visited_pages)
        return most_visited_pages
fetch_visited_pages_365days("ya29.c.b0Aaekm1JE4kHLWp0ln2JpY-1AI_jfpLP7igCb26VzaXCfE4RtoOaxBsKcOeE9-f6wu8lqSNbrqdRbTSMHx0-qWuEggcfQyM80DspcRVSXEr0RMV5IcHIHU7fasTz3P7tnZYY-wniAjageOKHkZFMeD7swaLIdFErzVH418jcKngzaW_6Qdnlr8KeJLQNAXNj4oMHg4YzaZyrwH2N-pp9K55JDpayUm2NMpvxjqHdgyZXLpWt0E7ZZCmn5VnKoisVTSqXejc7JUysUDT7N0r0PsZP9MuTilGhoCTJuwBMVDiNvjZWv-HX75RibP-oKZ-HEZNqQeU_fE337Krkxpgl7VqJMJqivx_hvXMYetpIJtZWZqbsQ95u5bRbuXjxUsMUIeF5MmO8W2UzMs9vc2gjnW--gwqtjmZl_pdRI2rUpqYeiQvls_s1wIW575bMByMMcMr45sryo8qi97areM3Yl1n9UIaoxkipZt-oVtgRb00uS25mMooV1VZQfm4YgmWBi6nSrBvoduB2n-rkiQv88jXfQ_svbqjpskk3u1M6amajmYwQS8SWlq-0nS-rdjorlYdoskBZo6W96fzaUeuveIJZnmx5f0RXbwVjvXxbtQ0QJWJoo84uMhhJ69x-ZhJqiBxmYurf1Brs_iehm-tm58Vh37XWM6k8wtvJha6mj7gnqOch_-itWOMt80q773c4u7w5w20Zg_tfrIc48JiXWdkw_Xu4IQ5Xtmg3f_kfckVhx0o6kyjxl-V3UfJXnbqh1rYX1Jv182XUsxQprxxOyslcofwkd2XV6zQM46-UvruWwgf1R6ef8JvjoZJIQudkmfx5116-tjvyo5Ogsp70iBVyOs0d-jmFRoh0yxetY5v6udW7Obf52lcZjs0OiXJwqzlBQ923zRfkQRQ1jd0gY69J5Y1hl2BbRsW24Og-kjiy4RoSw58kSmxem05dVbfY_joVqlJv6WbepQ3okrXqFy7rFaalQYu8FgwB-9Z1g6nVjt6nb5BJ6B_X")