import re


def extract_params_value(url):
    pattern = r"params=(\d+)"
    match = re.search(pattern, url)
    if match:
        return int(match.group(1))
    else:
        return None


def params_tag(log_info):
    tags = []
    for log_entry in log_info:
        if (
            log_entry.get("response") == 200
            and "/geoentity-services" in log_entry.get("url", "")
            and "params" in log_entry.get("url", "")
            and "values" in log_entry.get("url", "")
        ):
            value = extract_params_value(log_entry.get("url", ""))
            if value is not None:
                tags.append(
                    {
                        "req_id": log_entry.get("id"),
                        "tag_type": "params",
                        "value": value,
                        "timestamp": log_entry.get("timestamp", ""),
                    }
                )
    return tags



def service_tag(log_info):
    tags = []

    for log_entry in log_info:
        url = log_entry.get('url', '').strip()
        response = log_entry.get('response')

        if response == 200 and url and '/' in url:
            parts = url.split('/', 2)
            if len(parts) > 1: 
                common_url = parts[1]
                if common_url and not common_url.startswith(('.', '?')):  
                    tags.append({
                        "req_id": log_entry.get('id'),
                        "tag_type": "base_url",
                        "value": common_url,
                        "timestamp": log_entry.get('timestamp')
                    })

    return tags



def source_tag(log_info):
    tags = []
    for log_entry in log_info:
        if log_entry.get("response") == 200 and log_entry.get("referer"):
            referer_match = re.match(r"^https?://([^:/]+)", log_entry["referer"])
            if referer_match:
                domain = referer_match.group(1)
                if not re.match(r"^\d+\.\d+\.\d+\.\d+$", domain):
                    tags.append(
                        {
                            "req_id": log_entry["id"],
                            "tag_type": "domain",
                            "value": domain,
                            "timestamp": log_entry["timestamp"],
                        }
                    )
    return tags



def application_tag(log_info):
    tags = []
    for log_entry in log_info:
        if (
            log_entry.get("referer")
            and (
                log_entry["referer"].endswith(".jsp")
                or log_entry["referer"].endswith(".html")
            )
            and log_entry["response"] == 200
        ):
            last_part = re.search(r"https?://[^/]+(/[^?]+)", log_entry["referer"])
            if last_part:
                last_part = last_part.group(1)
                tags.append(
                    {
                        "req_id": log_entry["id"],
                        "tag_type": "referrer",
                        "value": last_part,
                        "timestamp": log_entry["timestamp"],
                    }
                )
    return tags


tag_generators = [params_tag, service_tag, source_tag, application_tag]
