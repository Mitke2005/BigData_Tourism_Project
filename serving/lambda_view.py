"""
CÔNG THỨC HỢP NHẤT LAMBDA VIEW THUẦN TÚY (LAMBDA ARCHITECTURE)
=============================================================================
Đảm nhiệm việc hợp nhất giữa:
1. Batch View (Tính từ HDFS & Master Dataset bởi Spark Batch Layer)
2. Real-time Reviews (Từ topic tourism-reviews của Kafka Speed Layer)
Hàm thuần Python không phụ thuộc Kafka/DB, cho phép chạy Unit Test độc lập.
=============================================================================
"""

import os
from typing import Dict, List, Set, Any, Optional

import re

DEFAULT_PRIOR_WEIGHT = float(os.getenv("RATING_PRIOR_WEIGHT", "5.0"))

def sanitize_text(text: Any) -> str:
    if not text:
        return ""
    s = str(text)
    s = re.sub(r'<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>', '', s, flags=re.IGNORECASE)
    s = re.sub(r'<[^>]+>', '', s)
    return s.strip()

def compute_spot_lambda_view(
    spot_state: Dict[str, Any],
    reviews_list: List[Dict[str, Any]],
    synced_event_ids: Set[str],
    prior_weight: Optional[float] = None
) -> Dict[str, Any]:
    """
    Hợp nhất một điểm đến theo công thức chuẩn:
    - Review 'đã đồng bộ' = event_id nằm trong batch_commit.synced_event_ids
    - Replay cùng event_id được khử trùng (idempotent, không tăng đếm)
    - total_sum   = review_sum + tổng rating các review KHÔNG thuộc synced_event_ids
    - total_count = review_count + số review đó
    - display_rating = round((base_rating*N0 + total_sum)/(N0 + total_count), 2), N0 = 5
    - Nếu không có review: display_rating = base_rating
    """
    n0 = float(prior_weight) if prior_weight is not None else float(os.getenv("RATING_PRIOR_WEIGHT", str(DEFAULT_PRIOR_WEIGHT)))
    
    stt = int(spot_state.get("stt", 0))
    base_rating = float(spot_state.get("base_rating", spot_state.get("rating", 4.5)))
    batch_review_sum = float(spot_state.get("review_sum", 0.0))
    batch_review_count = int(spot_state.get("review_count", 0))
    
    # 1. Khử trùng review theo event_id (last-wins) và lọc bỏ mã HTML độc hại
    deduped_reviews: Dict[str, Dict[str, Any]] = {}
    for r in reviews_list:
        ev_id = str(r.get("event_id", "")).strip()
        if ev_id:
            cr = dict(r)
            cr["user_name"] = sanitize_text(cr.get("user_name", "Khách du lịch")) or "Khách du lịch"
            cr["review_text"] = sanitize_text(cr.get("review_text", ""))
            deduped_reviews[ev_id] = cr
            
    # Sắp xếp các review theo timestamp giảm dần (mới nhất lên đầu)
    all_sorted_reviews = sorted(
        deduped_reviews.values(),
        key=lambda x: str(x.get("timestamp", "")),
        reverse=True
    )
    
    latest_review = all_sorted_reviews[0] if all_sorted_reviews else None
    if not latest_review and spot_state.get("latest_review"):
        lr = dict(spot_state["latest_review"])
        lr["user_name"] = sanitize_text(lr.get("user_name", "Khách du lịch")) or "Khách du lịch"
        lr["review_text"] = sanitize_text(lr.get("review_text", ""))
        latest_review = lr
    
    # 2. Lọc ra các review realtime CHƯA nằm trong synced_event_ids
    un_synced_reviews = [
        r for r in all_sorted_reviews
        if str(r.get("event_id", "")).strip() not in synced_event_ids
    ]
    
    rt_count = len(un_synced_reviews)
    rt_sum = sum(float(r.get("review_rating") if r.get("review_rating") is not None else r.get("rating", 5.0)) for r in un_synced_reviews)
    
    total_count = batch_review_count + rt_count
    total_sum = batch_review_sum + rt_sum
    
    # 3. Tính display_rating theo trọng số prior Bayes
    if total_count == 0:
        display_rating = round(base_rating, 2)
    else:
        display_rating = round((base_rating * n0 + total_sum) / (n0 + total_count), 2)
        # Giới hạn an toàn trong [1.0, 5.0]
        display_rating = max(1.0, min(5.0, display_rating))
        
    return {
        "stt": stt,
        "name": spot_state.get("name", f"Điểm đến #{stt}"),
        "location": spot_state.get("location", ""),
        "latitude": float(spot_state.get("latitude", 0.0)),
        "longitude": float(spot_state.get("longitude", 0.0)),
        "base_rating": base_rating,
        "rating": display_rating,
        "review_count": total_count,
        "batch_review_count": batch_review_count,
        "realtime_review_count": rt_count,
        "total_rating_sum": round(total_sum, 2),
        "latest_review": latest_review,
        "reviews": un_synced_reviews,
        "all_reviews": all_sorted_reviews,
        "cluster_id": int(spot_state.get("cluster_id", 0)),
        "cluster_name": spot_state.get("cluster_name", ""),
        "cluster_color": spot_state.get("cluster_color", "#2563EB"),
        "keywords": spot_state.get("keywords", []),
        "description": spot_state.get("description", ""),
        "image": spot_state.get("image", ""),
        "tfidf_recommendations": spot_state.get("tfidf_recommendations", []),
        "fp_growth": spot_state.get("fp_growth", {}),
        "job_id": spot_state.get("job_id", ""),
        "data_source": "Kafka Materialized Lambda View (RAM)"
    }
