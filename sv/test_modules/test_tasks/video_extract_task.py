import os
from typing import Dict, Any

from sv.task.task_base import TaskBase
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

class VideoFrameExtractionTask(TaskBase):
    """비디오 프레임 추출 작업 (로그 기반)"""

    def __init__(self, frame_interval: int = 2):
        """
        Args:
            frame_interval: 프레임 추출 간격 (초 단위)
        """
        super().__init__("VideoFrameExtractionTask")
        self.frame_interval = frame_interval

    def before_execute(self, context: Dict[str, Any]) -> None:
        """작업 실행 전 준비 작업"""
        try:
            # job_work_dir과 loop_context 가져오기
            job_work_dir = context.get('job_work_dir')
            loop_context = context.get('loop_context', {})

            if not job_work_dir:
                logger.error("❌ job_work_dir이 context에 없습니다")
                return

            # task_name과 start_time으로 작업 폴더명 생성
            task_work_folder_name = f"{self.task_name}"
            task_work_dir = os.path.join(job_work_dir, task_work_folder_name)

            # 작업 폴더 생성
            try:
                os.makedirs(task_work_dir, exist_ok=True)
                logger.info(f"✓ Task work directory created: {task_work_dir}")
            except Exception as e:
                logger.error(f"❌ Failed to create task work directory: {str(e)}", exc_info=True)
                return

            # context에 task_work_dir 저장
            context['task_work_dir'] = task_work_dir

            # 현재 data 정보 로깅
            current_data: Dict = context.get('current_data', {})
            video_url = current_data.get('video_url', 'unknown')

            logger.info(f"🎬 비디오 프레임 추출 준비 시작")
            logger.info(f"   📁 작업 폴더: {task_work_dir}")
            logger.info(f"   🎥 비디오: {video_url}")
            logger.info(f"   ⏱️  간격: {self.frame_interval}초")

        except Exception as e:
            logger.error(f"❌ before_execute 에러 발생: {str(e)}", exc_info=True)

    def _execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """실제 작업 실행"""
        current_data: Dict = context.get('current_data', {})
        task_work_dir = context.get('task_work_dir', '/tmp/video_frames')

        video_url = current_data.get('video_url')
        if not video_url:
            raise ValueError("video_url이 context에 포함되어야 합니다")

        # 기본 경로 설정 (task_work_dir을 기본 경로로 사용)
        video_name = current_data.get('video_name', 'video')
        extracted_frames_dir = os.path.join(task_work_dir, f"{video_name}.extracted")
        selected_frames_dir = os.path.join(task_work_dir, f"{video_name}.selected")

        logger.info(f"▶️  비디오 처리 시작")
        logger.info(f"   📁 입력 비디오: {video_url}")
        logger.info(f"   📁 추출 프레임 저장 경로: {extracted_frames_dir}")
        logger.info(f"   📁 선별된 프레임 저장 경로: {selected_frames_dir}")

        # 단계 1: 비디오 프레임을 PNG로 추출
        logger.info(f"⏳ 단계 1/3: 비디오 프레임 PNG 변환 중...")
        frame_count = 150  # 시뮬레이션: 150개 프레임
        logger.info(f"   ✓ {frame_count}개의 프레임을 PNG로 변환했습니다")
        logger.info(f"   ✓ 저장 위치: {extracted_frames_dir}/frame_*.png")

        # 단계 2: 일정 간격으로 프레임 선별
        logger.info(f"⏳ 단계 2/3: {self.frame_interval}초 간격으로 프레임 선별 중...")
        fps = 30  # 가정: 30fps
        frame_step = fps * self.frame_interval  # 프레임 간격
        selected_frames = []

        for i in range(0, frame_count, frame_step):
            frame_name = f"frame_{i:06d}.png"
            selected_frames.append(frame_name)
            logger.debug(f"   ✓ {frame_name} 선택 (시간: {(i / fps):.2f}초)")

        logger.info(f"   ✓ 총 {len(selected_frames)}개의 프레임 선별 완료")

        # 단계 3: 선별된 프레임을 특정 경로에 복사
        logger.info(f"⏳ 단계 3/3: 선별된 프레임 복사 중...")
        copied_frames_info = []
        for frame_name in selected_frames:
            src_path = f"{extracted_frames_dir}/{frame_name}"
            dst_path = f"{selected_frames_dir}/{frame_name}"
            logger.debug(f"   ✓ 복사: {src_path} -> {dst_path}")
            copied_frames_info.append(dst_path)

        logger.info(f"   ✓ {len(copied_frames_info)}개 프레임 복사 완료")

        # 결과 반환
        result = {
            "video_url": video_url,
            "video_name": video_name,
            "total_frames_extracted": frame_count,
            "selected_frames_count": len(selected_frames),
            "extracted_frames_dir": extracted_frames_dir,
            "selected_frames_dir": selected_frames_dir,
            "selected_frame_paths": copied_frames_info,
            "frame_interval_seconds": self.frame_interval,
        }

        return result

    def after_execute(self, context: Dict[str, Any], result: Dict[str, Any]) -> None:
        """작업 실행 후 정리 작업"""
        video_name = result.get('video_name', 'unknown')
        total_extracted = result.get('total_frames_extracted', 0)
        total_selected = result.get('selected_frames_count', 0)

        logger.info(f"✅ 비디오 프레임 추출 작업 완료")
        logger.info(f"   📊 추출된 프레임: {total_extracted}개")
        logger.info(f"   📊 선별된 프레임: {total_selected}개")
        logger.info(f"   📁 결과 경로: {result.get('selected_frames_dir')}")

    def on_error(self, context: Dict[str, Any], error: Exception) -> None:
        """에러 발생 시 처리"""
        video_url = context.get('video_url', 'unknown')
        logger.error(f"❌ 비디오 프레임 추출 작업 실패: {video_url}")
        logger.error(f"   에러 메시지: {str(error)}", exc_info=True)