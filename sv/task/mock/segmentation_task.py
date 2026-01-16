from typing import Dict, Any
import os
from sv.task.task_base import TaskBase
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

class VideoSegmentationTask(TaskBase):
    """비디오 세그멘테이션 작업 (로그 기반)"""

    def __init__(
        self, 
        model_name: str = "SegmentationModel_v1",
        delay_seconds: float = None,
        raise_exception: Exception = None
    ):
        """
        Args:
            model_name: 사용할 모델 이름
            delay_seconds: 작업 실행 시 지연 시간(초). None이면 지연 없음
            raise_exception: 작업 실행 시 발생시킬 예외. None이면 정상 실행
        """
        super().__init__("VideoSegmentationTask", delay_seconds, raise_exception)
        self.model_name = model_name

    def before_execute(self, context: Dict[str, Any]) -> None:
        """작업 실행 전 준비 작업"""
        try:
            # job_work_dir과 loop_context 가져오기
            job_work_dir = context.get('job_dir')

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

            logger.info(f"🎯 비디오 세그멘테이션 준비 시작")
            logger.info(f"   📁 작업 폴더: {task_work_dir}")
            logger.info(f"   🤖 모델: {self.model_name}")

        except Exception as e:
            logger.error(f"❌ before_execute 에러 발생: {str(e)}", exc_info=True)

    def _execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """실제 작업 실행"""
        # 이전 task의 결과 가져오기
        previous_task_result = context.get('VideoFrameExtractionTask', {})
        copied_frames_info = previous_task_result.get('selected_frame_paths', [])

        if not copied_frames_info:
            raise ValueError("VideoFrameExtractionTask 결과에서 selected_frame_paths를 찾을 수 없습니다")

        task_work_dir = context.get('task_work_dir', '/tmp/segmentation')
        segmentation_output_dir = os.path.join(task_work_dir, "segmentation_output")
        segmentation_selected_dir = os.path.join(task_work_dir, "segmentation_selected")

        logger.info(f"▶️  비디오 세그멘테이션 처리 시작")
        logger.info(f"   📁 입력 프레임 수: {len(copied_frames_info)}개")
        logger.info(f"   📁 세그멘테이션 출력 경로: {segmentation_output_dir}")
        logger.info(f"   📁 선별된 세그멘테이션 경로: {segmentation_selected_dir}")

        # 단계 1: 모델 로드
        logger.info(f"⏳ 단계 1/4: 모델 로드 중...")
        logger.info(f"   ✓ 모델 로드 완료: {self.model_name}")
        logger.info(f"   ✓ 모델 설정: GPU 활성화, 배치 크기=8")

        # 단계 2: 각 프레임에 대해 세그멘테이션 실행
        logger.info(f"⏳ 단계 2/4: 세그멘테이션 추론 중...")
        segmentation_results = []
        successful_segmentations = 0

        for idx, frame_path in enumerate(copied_frames_info, 1):
            frame_name = os.path.basename(frame_path)

            try:
                # 모델 실행 (로그만)
                logger.debug(f"   ▶️  [{idx}/{len(copied_frames_info)}] 처리 중: {frame_name}")

                # 시뮬레이션: 일부 프레임만 segmentation 성공
                has_segmentation = (idx % 3) != 0  # 3의 배수가 아닌 것만 성공 (가상)

                if has_segmentation:
                    # 원본 프레임 경로
                    original_frame_path = frame_path
                    
                    # Mask 파일 생성 (가상)
                    mask_file = f"{os.path.splitext(frame_name)[0]}_mask.png"
                    mask_path = os.path.join(segmentation_output_dir, mask_file)

                    logger.debug(f"   ✓ 세그멘테이션 성공: {frame_name}")
                    logger.debug(f"     - 원본 이미지: {frame_name}")
                    logger.debug(f"     - Mask 파일: {mask_file}")
                    logger.debug(f"     - 클래스 개수: 2개")
                    logger.debug(f"     - 신뢰도 점수: 0.94")

                    segmentation_results.append({
                        'frame': frame_name,
                        'original_frame_path': original_frame_path,
                        'mask_file': mask_file,
                        'mask_path': mask_path,
                        'has_segmentation': True,
                        'confidence': 0.94
                    })
                    successful_segmentations += 1
                else:
                    logger.debug(f"   ⚠️  세그멘테이션 대상 없음: {frame_name}")
                    segmentation_results.append({
                        'frame': frame_name,
                        'original_frame_path': frame_path,
                        'has_segmentation': False,
                        'reason': '유의미한 객체 없음'
                    })

            except Exception as e:
                logger.warning(f"   ❌ 세그멘테이션 실패: {frame_name} - {str(e)}")
                segmentation_results.append({
                    'frame': frame_name,
                    'original_frame_path': frame_path,
                    'has_segmentation': False,
                    'error': str(e)
                })

        logger.info(f"   ✓ 세그멘테이션 추론 완료: {successful_segmentations}/{len(copied_frames_info)}개 성공")

        # 단계 3: 세그멘테이션이 추출된 결과 선별
        logger.info(f"⏳ 단계 3/4: 세그멘테이션 결과 선별 중...")
        selected_segmentations = [r for r in segmentation_results if r.get('has_segmentation', False)]
        selected_segmentation_pairs = []
        
        for seg_result in selected_segmentations:
            original_frame_path = seg_result.get('original_frame_path')
            mask_path = seg_result.get('mask_path')
            frame_name = seg_result.get('frame')
            
            if original_frame_path and mask_path:
                logger.debug(f"   ✓ 선별: {frame_name}")
                logger.debug(f"     - 원본 프레임: {original_frame_path}")
                logger.debug(f"     - Mask 경로: {mask_path}")
                selected_segmentation_pairs.append({
                    'original_frame_path': original_frame_path,
                    'mask_path': mask_path
                })
        
        logger.info(f"   ✓ 총 {len(selected_segmentations)}개 세그멘테이션 선별 완료")
        
        # 단계 4: 선별된 원본 프레임과 마스크 복사
        logger.info(f"⏳ 단계 4/4: 선별된 프레임과 마스크 복사 중...")
        final_segmentation_pairs = []
        
        for pair in selected_segmentation_pairs:
            original_frame = pair['original_frame_path']
            mask_path = pair['mask_path']
            
            # 원본 프레임 복사
            original_filename = os.path.basename(original_frame)
            final_original_path = os.path.join(segmentation_selected_dir, original_filename)
            logger.debug(f"   ✓ 원본 복사: {original_frame} -> {final_original_path}")
            
            # 마스크 복사
            mask_filename = os.path.basename(mask_path)
            final_mask_path = os.path.join(segmentation_selected_dir, mask_filename)
            logger.debug(f"   ✓ 마스크 복사: {mask_path} -> {final_mask_path}")
            
            final_segmentation_pairs.append({
                'original_frame_path': final_original_path,
                'mask_path': final_mask_path
            })
        
        logger.info(f"   ✓ {len(final_segmentation_pairs)}개 세그멘테이션 쌍(원본+마스크) 복사 완료")

        # 결과 반환
        result = {
            "model_name": self.model_name,
            "input_frames_count": len(copied_frames_info),
            "segmentation_output_dir": segmentation_output_dir,
            "segmentation_selected_dir": segmentation_selected_dir,
            "total_segmentations": len(segmentation_results),
            "successful_segmentations": successful_segmentations,
            "selected_segmentations_count": len(selected_segmentations),
            "selected_segmentation_pairs": final_segmentation_pairs,
            "segmentation_results": segmentation_results,
        }

        return result

    def after_execute(self, context: Dict[str, Any], result: Dict[str, Any]) -> None:
        """작업 실행 후 정리 작업"""
        input_count = result.get('input_frames_count', 0)
        successful = result.get('successful_segmentations', 0)
        selected = result.get('selected_segmentations_count', 0)

        logger.info(f"✅ 비디오 세그멘테이션 작업 완료")
        logger.info(f"   📊 입력 프레임: {input_count}개")
        logger.info(f"   📊 세그멘테이션 성공: {successful}개")
        logger.info(f"   📊 선별된 세그멘테이션: {selected}개")
        logger.info(f"   📁 결과 경로: {result.get('segmentation_selected_dir')}")

    def on_error(self, context: Dict[str, Any], error: Exception) -> None:
        """에러 발생 시 처리"""
        logger.error(f"❌ 비디오 세그멘테이션 작업 실패")
        logger.error(f"   에러 메시지: {str(error)}", exc_info=True)