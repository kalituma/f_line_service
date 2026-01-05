"""
Ray를 사용하여 비디오 데이터를 업데이트하는 daemon 모듈

frfr_info_id: int로 변환 후 1을 더함
analysis_id: yyyyMMdd_hhmm_VIDEO_xxx 형식에서 minute(mm)에 1을 더함

예시:
  frfr_info_id: 186525 -> 186526
  analysis_id: 20251222_1706_VIDEO_003 -> 20251222_1707_VIDEO_003
"""

import ray
from typing import Dict, Any
import logging
from datetime import datetime, timedelta
import os
import sys

from server.backend.service.video_service import WildfireVideoService
from server.backend.service.analysis_status_service import AnalysisStatusService
from server.backend.service.fire_info_service import FireInfoService

# Ray Actor 내부 로깅 설정
logger = logging.getLogger(__name__)


def setup_actor_logging(debug: bool = False) -> logging.Logger:
    """
    Ray Actor 내부에서 사용할 logger 설정
    파일과 콘솔에 모두 출력
    """
    actor_logger = logging.getLogger(f"VideoDaemonActor-{os.getpid()}")
    
    # 기존 핸들러 제거
    for handler in actor_logger.handlers[:]:
        actor_logger.removeHandler(handler)
    
    level = logging.DEBUG if debug else logging.INFO
    actor_logger.setLevel(level)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(
        '[%(asctime)s] [Ray-Actor] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    
    # 파일 핸들러
    log_file = f"ray_daemon_actor_{os.getpid()}.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    file_formatter = logging.Formatter(
        '[%(asctime)s] [Ray-Actor-%(process)d] [%(levelname)s] %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    actor_logger.addHandler(console_handler)
    actor_logger.addHandler(file_handler)
    
    return actor_logger


@ray.remote
class VideoDaemonActor:
    """Ray Actor: 비디오, 분석 상태, 산불 정보 데이터 업데이트 daemon"""
    
    def __init__(self, debug: bool = False):
        """
        Actor 초기화
        
        Args:
            debug: 디버그 모드 활성화 여부
        """
        self.debug = debug
        self.logger = setup_actor_logging(debug)
        
        self.logger.info("=" * 80)
        self.logger.info("VideoDaemonActor 초기화 시작")
        self.logger.info(f"Process ID: {os.getpid()}")
        self.logger.info(f"Debug Mode: {self.debug}")
        self.logger.info("=" * 80)
        
        try:
            self.video_service = WildfireVideoService()
            self.logger.info("✓ WildfireVideoService 초기화 완료")
            
            self.analysis_status_service = AnalysisStatusService()
            self.logger.info("✓ AnalysisStatusService 초기화 완료")
            
            self.fire_info_service = FireInfoService()
            self.logger.info("✓ FireInfoService 초기화 완료")
            
            self.logger.info("=" * 80)
            self.logger.info("✅ VideoDaemonActor 초기화 완료")
            self.logger.info("=" * 80)
        except Exception as e:
            self.logger.error(f"❌ Actor 초기화 실패: {str(e)}", exc_info=True)
            raise
    
    def get_latest_analysis_ids(self) -> Dict[str, Any]:
        """
        DB에서 가장 최근의 분석 데이터를 읽어와서 frfr_info_id와 analysis_id를 반환합니다.
        
        Returns:
            최근 분석 데이터의 ID 정보
            {
                "success": bool,
                "frfr_info_id": str,
                "analysis_id": str,
                "message": str,
                "video_count": int
            }
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("📊 최근 분석 데이터 조회 시작...")
            self.logger.info("=" * 80)
            
            # 모든 비디오 조회
            all_videos = self.video_service.get_all_videos()
            
            if not all_videos:
                error_msg = "DB에 비디오 데이터가 없습니다"
                self.logger.warning(f"⚠️  {error_msg}")
                return {
                    "success": False,
                    "message": error_msg,
                    "frfr_info_id": None,
                    "analysis_id": None,
                    "video_count": 0
                }
            
            self.logger.debug(f"총 비디오 레코드 수: {len(all_videos)}")
            
            # frfr_info_id를 int로 파싱하여 가장 큰 ID를 가진 데이터 찾기
            try:
                sorted_videos = sorted(
                    all_videos,
                    key=lambda v: int(v.frfr_info_id),
                    reverse=True
                )
                latest_video = sorted_videos[0]
                self.logger.debug(f"frfr_info_id 기준 정렬 완료: {len(sorted_videos)}개 비디오")
            except Exception as e:
                self.logger.debug(f"frfr_info_id 정렬 실패, 첫번째 비디오 사용: {str(e)}")
                latest_video = all_videos[0]
            
            frfr_info_id = latest_video.frfr_info_id
            analysis_id = latest_video.analysis_id
            
            self.logger.info(f"✓ 최근 분석 데이터 조회 완료")
            self.logger.info(f"  frfr_info_id: {frfr_info_id}")
            self.logger.info(f"  analysis_id: {analysis_id}")
            self.logger.info(f"  비디오명: {latest_video.video_name}")
            self.logger.info(f"  추가 시간: {latest_video.add_time}")
            
            return {
                "success": True,
                "message": f"최근 분석 데이터 조회 완료: {frfr_info_id}/{analysis_id}",
                "frfr_info_id": frfr_info_id,
                "analysis_id": analysis_id,
                "video_count": len(all_videos),
                "latest_video_name": latest_video.video_name,
                "latest_add_time": latest_video.add_time
            }
        
        except Exception as e:
            error_msg = f"최근 분석 데이터 조회 실패: {str(e)}"
            self.logger.error(f"❌ {error_msg}", exc_info=True)
            return {
                "success": False,
                "message": error_msg,
                "frfr_info_id": None,
                "analysis_id": None,
                "video_count": 0
            }
    
    def process_videos(
        self,
        original_frfr_info_id: str,
        original_analysis_id: str
    ) -> Dict[str, Any]:
        """
        비디오, 분석 상태, 산불 정보 데이터를 읽어와서 새로운 ID로 재삽입합니다.
        
        Args:
            original_frfr_info_id: 원본 산불 정보 ID
            original_analysis_id: 원본 분석 ID (형식: yyyyMMdd_hhmm_VIDEO_xxx)
        
        Returns:
            처리 결과 딕셔너리
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("📊 전체 데이터 처리 시작")
            self.logger.info("=" * 80)
            self.logger.info(
                f"📥 입력 파라미터: frfr_info_id={original_frfr_info_id}, "
                f"analysis_id={original_analysis_id}"
            )
            
            # frfr_info_id 업데이트 (1 증가)
            self.logger.debug(f"frfr_info_id 변환 시작: {original_frfr_info_id} (type: {type(original_frfr_info_id)})")
            new_frfr_info_id = int(original_frfr_info_id) + 1
            new_frfr_info_id_str = str(new_frfr_info_id)
            self.logger.info(f"✓ frfr_info_id 변환: {original_frfr_info_id} → {new_frfr_info_id}")
            
            # analysis_id 업데이트 (minute에 1을 더함)
            self.logger.debug(f"analysis_id 변환 시작: {original_analysis_id}")
            new_analysis_id = self._update_analysis_id(original_analysis_id)
            self.logger.info(f"✓ analysis_id 변환: {original_analysis_id} → {new_analysis_id}")
            
            # 결과 저장
            result = {
                "success": True,
                "original_frfr_info_id": original_frfr_info_id,
                "original_analysis_id": original_analysis_id,
                "new_frfr_info_id": new_frfr_info_id,
                "new_analysis_id": new_analysis_id,
                "videos": {"count": 0, "failed": 0, "errors": []},
                "analysis_status": {"count": 0, "failed": 0, "errors": []},
                "fire_info": {"success": False, "error": None},
                "errors": []
            }
            
            # ============== 1. 비디오 데이터 처리 ==============
            self.logger.info("=" * 80)
            self.logger.info("[1/3] 🎬 비디오 데이터 처리 시작...")
            self.logger.info("=" * 80)
            result["videos"] = self._process_video_data(
                original_frfr_info_id,
                original_analysis_id,
                new_frfr_info_id_str,
                new_analysis_id
            )
            self.logger.info(f"[1/3 결과] Videos: {result['videos']['count']} inserted, {result['videos']['failed']} failed")
            
            # ============== 2. 분석 상태 데이터 처리 ==============
            self.logger.info("=" * 80)
            self.logger.info("[2/3] 📊 분석 상태 데이터 처리 시작...")
            self.logger.info("=" * 80)
            result["analysis_status"] = self._process_analysis_status_data(
                original_frfr_info_id,
                original_analysis_id,
                new_frfr_info_id_str,
                new_analysis_id
            )
            self.logger.info(f"[2/3 결과] Analysis Status: {result['analysis_status']['count']} inserted, {result['analysis_status']['failed']} failed")
            
            # ============== 3. 산불 정보 데이터 처리 ==============
            self.logger.info("=" * 80)
            self.logger.info("[3/3] 🔥 산불 정보 데이터 처리 시작...")
            self.logger.info("=" * 80)
            result["fire_info"] = self._process_fire_info_data(
                original_frfr_info_id,
                new_frfr_info_id_str
            )
            self.logger.info(f"[3/3 결과] Fire Info: {'✓ Success' if result['fire_info']['success'] else '✗ Failed'}")
            
            # 최종 성공 여부 판단
            result["success"] = (
                result["videos"]["failed"] == 0 and
                result["analysis_status"]["failed"] == 0 and
                result["fire_info"]["success"]
            )
            
            result["message"] = (
                f"Videos: {result['videos']['count']}, "
                f"Analysis Status: {result['analysis_status']['count']}, "
                f"Fire Info: {'Success' if result['fire_info']['success'] else 'Failed'}"
            )
            
            self.logger.info("=" * 80)
            status_icon = "✅" if result["success"] else "⚠️"
            self.logger.info(f"{status_icon} 전체 데이터 처리 완료: {result['message']}")
            self.logger.info("=" * 80)
            
            return result
        
        except Exception as e:
            self.logger.error(f"❌ 데이터 처리 중 오류 발생: {str(e)}", exc_info=True)
            return {
                "success": False,
                "message": f"데이터 처리 중 오류 발생: {str(e)}",
                "videos": {"count": 0, "failed": 0, "errors": []},
                "analysis_status": {"count": 0, "failed": 0, "errors": []},
                "fire_info": {"success": False, "error": str(e)},
                "errors": [str(e)]
            }
    
    def _process_video_data(
        self,
        original_frfr_info_id: str,
        original_analysis_id: str,
        new_frfr_info_id: str,
        new_analysis_id: str
    ) -> Dict[str, Any]:
        """비디오 데이터 처리"""
        result = {"count": 0, "failed": 0, "errors": []}
        
        try:
            self.logger.debug(f"비디오 데이터 조회 시작: {original_frfr_info_id}/{original_analysis_id}")
            
            # 원본 비디오 데이터 조회
            original_videos = self.video_service.get_videos_by_frfr_id_and_analysis_id(
                original_frfr_info_id, original_analysis_id
            )
            
            if not original_videos:
                self.logger.warning(
                    f"⚠️  비디오 데이터 없음: {original_frfr_info_id}/{original_analysis_id}"
                )
                result["errors"].append(
                    f"No videos found for {original_frfr_info_id}/{original_analysis_id}"
                )
                return result
            
            self.logger.info(f"📥 비디오 조회 완료: {len(original_videos)}개의 비디오 발견")
            
            # 새로운 ID로 비디오 데이터 재삽입
            for idx, video in enumerate(original_videos, 1):
                try:
                    self.logger.debug(f"[비디오 {idx}/{len(original_videos)}] 처리 중: {video.video_name}")
                    
                    doc_id = self.video_service.wildfire_video_table.insert(
                        frfr_info_id=new_frfr_info_id,
                        analysis_id=new_analysis_id,
                        video_name=video.video_name,
                        video_type=video.video_type,
                        video_path=video.video_path,
                        add_time=video.add_time
                    )
                    result["count"] += 1
                    self.logger.info(
                        f"✓ [비디오 {idx}/{len(original_videos)}] 삽입 완료: "
                        f"{new_frfr_info_id}/{new_analysis_id}/{video.video_name}"
                    )
                except Exception as e:
                    result["failed"] += 1
                    error_msg = f"❌ 비디오 삽입 실패 {video.video_name}: {str(e)}"
                    result["errors"].append(error_msg)
                    self.logger.error(error_msg, exc_info=self.debug)
            
            self.logger.info(f"🎬 비디오 처리 완료: {result['count']}개 삽입, {result['failed']}개 실패")
            return result
        
        except Exception as e:
            error_msg = f"❌ 비디오 데이터 처리 실패: {str(e)}"
            result["errors"].append(error_msg)
            self.logger.error(error_msg, exc_info=True)
            return result
    
    def _process_analysis_status_data(
        self,
        original_frfr_info_id: str,
        original_analysis_id: str,
        new_frfr_info_id: str,
        new_analysis_id: str
    ) -> Dict[str, Any]:
        """분석 상태 데이터 처리"""
        result = {"count": 0, "failed": 0, "errors": []}
        
        try:
            self.logger.debug(f"분석 상태 데이터 조회 시작: {original_analysis_id}")
            
            # 원본 분석 상태 데이터 조회
            original_status_list = self.analysis_status_service.get_all_status_by_analysis_id(
                original_analysis_id
            )
            
            if not original_status_list:
                self.logger.warning(
                    f"⚠️  분석 상태 데이터 없음: {original_analysis_id}"
                )
                result["errors"].append(
                    f"No analysis status found for {original_analysis_id}"
                )
                return result
            
            self.logger.debug(f"전체 분석 상태 레코드: {len(original_status_list)}개")
            
            # frfr_info_id 매칭되는 데이터만 필터링
            matched_status = [
                status for status in original_status_list
                if status.get("frfr_info_id") == original_frfr_info_id
            ]
            
            if not matched_status:
                self.logger.warning(
                    f"⚠️  매칭되는 분석 상태 없음: {original_frfr_info_id}/{original_analysis_id}"
                )
                result["errors"].append(
                    f"No analysis status found for {original_frfr_info_id}/{original_analysis_id}"
                )
                return result
            
            self.logger.info(f"📥 분석 상태 조회 완료: {len(matched_status)}개의 분석 상태 발견")
            
            # 새로운 ID로 분석 상태 데이터 재삽입
            for idx, status in enumerate(matched_status, 1):
                try:
                    self.logger.debug(f"[분석상태 {idx}/{len(matched_status)}] 처리 중: {status['video_name']}")
                    
                    doc_id = self.analysis_status_service.analysis_status_table.insert(
                        analysis_id=new_analysis_id,
                        frfr_info_id=new_frfr_info_id,
                        video_name=status["video_name"],
                        analysis_status=status["analysis_status"]
                    )
                    result["count"] += 1
                    self.logger.info(
                        f"✓ [분석상태 {idx}/{len(matched_status)}] 삽입 완료: "
                        f"{new_frfr_info_id}/{new_analysis_id}/{status['video_name']}"
                    )
                except Exception as e:
                    result["failed"] += 1
                    error_msg = f"❌ 분석 상태 삽입 실패 {status['video_name']}: {str(e)}"
                    result["errors"].append(error_msg)
                    self.logger.error(error_msg, exc_info=self.debug)
            
            self.logger.info(f"📊 분석 상태 처리 완료: {result['count']}개 삽입, {result['failed']}개 실패")
            return result
        
        except Exception as e:
            error_msg = f"❌ 분석 상태 데이터 처리 실패: {str(e)}"
            result["errors"].append(error_msg)
            self.logger.error(error_msg, exc_info=True)
            return result
    
    def _process_fire_info_data(
        self,
        original_frfr_info_id: str,
        new_frfr_info_id: str
    ) -> Dict[str, Any]:
        """산불 정보 데이터 처리"""
        result = {"success": False, "error": None}
        
        try:
            self.logger.debug(f"산불 정보 데이터 조회 시작: {original_frfr_info_id}")
            
            # 원본 산불 정보 데이터 조회
            original_fire_info = self.fire_info_service.get_fire_location(
                original_frfr_info_id
            )
            
            if not original_fire_info:
                self.logger.warning(
                    f"⚠️  산불 정보 데이터 없음: {original_frfr_info_id}"
                )
                result["error"] = f"No fire location found for {original_frfr_info_id}"
                return result
            
            latitude = original_fire_info["fire_location"]["latitude"]
            longitude = original_fire_info["fire_location"]["longitude"]
            self.logger.info(f"📥 산불 정보 조회 완료: {original_frfr_info_id} ({latitude}, {longitude})")
            
            # 새로운 ID로 산불 정보 데이터 재삽입
            self.logger.debug(f"새로운 산불 정보 저장 시작: {new_frfr_info_id}")
            save_result = self.fire_info_service.save_fire_location({
                "frfr_info_id": new_frfr_info_id,
                "location": {
                    "latitude": latitude,
                    "longitude": longitude
                }
            })
            
            if save_result.get("success"):
                result["success"] = True
                self.logger.info(
                    f"✓ 산불 정보 삽입 완료: {new_frfr_info_id} ({latitude}, {longitude})"
                )
            else:
                result["error"] = save_result.get("error", "Unknown error")
                self.logger.error(f"❌ 산불 정보 삽입 실패: {result['error']}")
            
            return result
        
        except Exception as e:
            error_msg = f"❌ 산불 정보 데이터 처리 실패: {str(e)}"
            result["error"] = error_msg
            self.logger.error(error_msg, exc_info=True)
            return result
    
    def _update_analysis_id(self, analysis_id: str) -> str:
        """
        analysis_id의 minute 부분에 1을 더합니다.
        
        형식: yyyyMMdd_hhmm_VIDEO_xxx
        예: 20251222_1706_VIDEO_003 -> 20251222_1707_VIDEO_003
        
        Args:
            analysis_id: 원본 분석 ID
        
        Returns:
            업데이트된 분석 ID
        """
        try:
            self.logger.debug(f"analysis_id 파싱 시작: {analysis_id}")
            
            parts = analysis_id.split('_')
            if len(parts) < 3:
                self.logger.error(f"❌ 유효하지 않은 analysis_id 형식: {analysis_id}")
                return analysis_id
            
            date_part = parts[0]      # 20251222
            time_part = parts[1]      # 1706
            video_part = '_'.join(parts[2:])  # VIDEO_003
            
            self.logger.debug(f"  date_part: {date_part}, time_part: {time_part}, video_part: {video_part}")
            
            # 시간과 분 추출
            hh = time_part[:2]        # 17
            mm = time_part[2:]        # 06
            
            self.logger.debug(f"  시간: {hh}, 분: {mm}")
            
            # 분에 1을 더하기
            mm_int = int(mm)
            new_mm = mm_int + 1
            
            # 분이 60 이상이면 시간 증가
            hh_int = int(hh)
            if new_mm >= 60:
                self.logger.debug(f"  분이 60 이상({new_mm}): 시간 증가")
                new_mm = 0
                hh_int += 1
                
                # 시간이 24 이상이면 날짜 증가
                if hh_int >= 24:
                    self.logger.debug(f"  시간이 24 이상({hh_int}): 날짜 증가")
                    date_obj = datetime.strptime(date_part, "%Y%m%d")
                    date_obj += timedelta(days=1)
                    date_part = date_obj.strftime("%Y%m%d")
                    hh_int = 0
            
            new_hh = str(hh_int).zfill(2)
            new_mm_str = str(new_mm).zfill(2)
            new_time_part = new_hh + new_mm_str
            
            new_analysis_id = f"{date_part}_{new_time_part}_{video_part}"
            self.logger.debug(f"  변환 결과: {new_analysis_id}")
            self.logger.info(f"✓ analysis_id 변환: {analysis_id} → {new_analysis_id}")
            return new_analysis_id
        
        except Exception as e:
            self.logger.error(f"❌ analysis_id 변환 실패: {str(e)}", exc_info=self.debug)
            return analysis_id

