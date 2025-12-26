from server.response_obj import WildfireResponse, FireLocation, VideoInfo

def generate_mock_response(frfr_info_id: str, analysis_id: str) -> WildfireResponse:
    """
    주어진 frfr_info_id와 analysis_id로 모의 응답 데이터를 생성합니다.
    """
    fire_location = FireLocation(
        longitude=128.45275734365313,
        latitude=36.30740755588261
    )

    base_path = f"/data/helivid/hv_proc/{frfr_info_id}"

    videos = [
        VideoInfo(
            path=f"{base_path}/FPA601/FPA630_20251127_190145_7366_000.mkv",
            name="FPA630_20251127_190145_7366_000.mkv",
            add_time="2025-11-27 10:03:47.550",
            analysis_status="STAT_001"
        ),
        VideoInfo(
            path=f"{base_path}/FPA601/FPA630_20251127_190318_1478_000.mkv",
            name="FPA630_20251127_190318_1478_000.mkv",
            add_time="2025-11-27 10:03:58.703",
            analysis_status="STAT_001"
        ),
        VideoInfo(
            path=f"{base_path}/FPA601/FPA630_20251127_190115_5080_000.mkv",
            name="FPA630_20251127_190115_5080_000.mkv",
            add_time="2025-11-27 10:04:04.782",
            analysis_status="STAT_001"
        ),
        VideoInfo(
            path=f"{base_path}/FPA601/FPA630_20251127_190214_8753_000.mkv",
            name="FPA630_20251127_190214_8753_000.mkv",
            add_time="2025-11-27 10:04:10.894",
            analysis_status="STAT_001"
        ),
        VideoInfo(
            path=f"{base_path}/FPA630/FPA630_20251127_190247_0401_000.mkv",
            name="FPA630_20251127_190247_0401_000.mkv",
            add_time="2025-11-27 10:04:16.965",
            analysis_status="STAT_001"
        ),
    ]

    return WildfireResponse(
        frfr_info_id=frfr_info_id,
        analysis_id=analysis_id,
        fire_location=fire_location,
        videos=videos
    )