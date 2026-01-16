from typing import Dict, Any, List
import os
from sv.task.task_base import TaskBase
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)


class FeatureMatchingTask(TaskBase):
    """특징점 매칭 및 Homography 산출 작업 (로그 기반)"""

    def __init__(
        self, 
        matcher_model_name: str = "FeatureMatcher_v1", 
        min_matches: int = 4,
        delay_seconds: float = None,
        raise_exception: Exception = None
    ):
        """
        Args:
            matcher_model_name: 특징점 매칭 모델 이름
            min_matches: Homography 산출 필요한 최소 특징점 수
            delay_seconds: 작업 실행 시 지연 시간(초). None이면 지연 없음
            raise_exception: 작업 실행 시 발생시킬 예외. None이면 정상 실행
        """
        super().__init__("FeatureMatchingTask", delay_seconds, raise_exception)
        self.matcher_model_name = matcher_model_name
        self.min_matches = min_matches

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

            logger.info(f"🔍 특징점 매칭 및 Homography 산출 준비 시작")
            logger.info(f"   📁 작업 폴더: {task_work_dir}")
            logger.info(f"   🤖 모델: {self.matcher_model_name}")
            logger.info(f"   🎯 최소 특징점 수: {self.min_matches}개")

        except Exception as e:
            logger.error(f"❌ before_execute 에러 발생: {str(e)}", exc_info=True)

    def _execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """실제 작업 실행"""
        # SegmentationTask 결과에서 원본 영상 경로 가져오기
        segmentation_result = context.get('VideoSegmentationTask', {})
        selected_segmentation_pairs = segmentation_result.get('selected_segmentation_pairs', [])

        if not selected_segmentation_pairs:
            raise ValueError("VideoSegmentationTask 결과에서 selected_segmentation_pairs를 찾을 수 없습니다")

        # LocationSimulationTask 결과에서 생성된 프레임 경로 가져오기
        location_simulation_result = context.get('LocationSimulationTask', {})
        location_simulation_results = location_simulation_result.get('location_simulation_results', [])

        if not location_simulation_results:
            raise ValueError("LocationSimulationTask 결과에서 location_simulation_results를 찾을 수 없습니다")

        task_work_dir = context.get('task_work_dir', '/tmp/feature_matching')
        matching_output_dir = os.path.join(task_work_dir, "matching_output")
        homography_output_dir = os.path.join(task_work_dir, "homography_matrices")

        logger.info(f"▶️  특징점 매칭 처리 시작")
        logger.info(f"   📊 선별 원본 영상 수: {len(selected_segmentation_pairs)}개")
        logger.info(f"   📊 생성된 시뮬레이션 위치 수: {len(location_simulation_results)}개")
        logger.info(f"   📁 매칭 결과 경로: {matching_output_dir}")
        logger.info(f"   📁 Homography 행렬 경로: {homography_output_dir}")

        # 단계 1: 특징점 매칭 모델 로드
        logger.info(f"⏳ 단계 1/4: 특징점 매칭 모델 로드 중...")
        logger.info(f"   ✓ 모델 로드 완료: {self.matcher_model_name}")
        logger.info(f"   ✓ 모델 설정: 특징점 검출기=SIFT, 매칭기=BFMatcher")

        # 단계 2: 각 위치별로 원본 영상과 시뮬레이션 영상 매칭
        logger.info(f"⏳ 단계 2/4: 특징점 추출 및 매칭 중...")
        matching_results = []

        for seg_idx, original_pair in enumerate(selected_segmentation_pairs, 1):
            original_frame_path = original_pair.get('original_frame_path')
            frame_name = os.path.basename(original_frame_path)

            try:
                logger.debug(f"   ▶️  [{seg_idx}/{len(selected_segmentation_pairs)}] 매칭 중: {frame_name}")

                # 해당 위치의 시뮬레이션 프레임 찾기
                matching_location_result = None
                for loc_result in location_simulation_results:
                    if loc_result.get('segmentation_index') == seg_idx:
                        matching_location_result = loc_result
                        break

                if not matching_location_result:
                    logger.warning(f"   ⚠️  일치하는 시뮬레이션 위치 없음: {frame_name}")
                    matching_results.append({
                        'segmentation_index': seg_idx,
                        'frame_name': frame_name,
                        'original_frame_path': original_frame_path,
                        'has_match': False,
                        'reason': '일치하는 시뮬레이션 위치 없음'
                    })
                    continue

                inferred_address = matching_location_result.get('inferred_address')
                generated_frames = matching_location_result.get('generated_frames', [])

                logger.debug(f"   ✓ 원본 영상: {frame_name}")
                logger.debug(f"     - 위치: {inferred_address}")
                logger.debug(f"     - 시뮬레이션 프레임 수: {len(generated_frames)}개")

                # 단계 2-1: 특징점 추출
                logger.debug(f"   ▶️  특징점 추출 중...")
                keypoints_original = 125  # 시뮬레이션: 원본에서 추출된 특징점 수
                logger.debug(f"     - 원본 영상 특징점: {keypoints_original}개")

                # 각 시뮬레이션 프레임과 매칭
                frame_matches = []
                for frame_idx, sim_frame_path_dict in enumerate(generated_frames, 1):
                    try:
                        sim_frame_path = sim_frame_path_dict.get("rgb_path")
                        sim_frame_name = os.path.basename(sim_frame_path)

                        # 특징점 추출
                        keypoints_sim = 98 + (frame_idx * 2)  # 시뮬레이션
                        logger.debug(f"     - 시뮬레이션 프레임 {frame_idx} 특징점: {keypoints_sim}개")

                        # 특징점 매칭 (처음 1~2개 프레임만 충분한 특징점 매칭, 나머지는 부족)
                        if frame_idx <= 2:  # 처음 1~2개 프레임에서만 충분한 특징점 매칭
                            matched_keypoints = 15  # 충분한 특징점 (min_matches가 4 이상이므로 homography 산출 가능)
                        else:
                            matched_keypoints = 1  # 부족한 특징점
                        logger.debug(f"     - 매칭된 특징점: {matched_keypoints}개")

                        match_ratio = (matched_keypoints / min(keypoints_original, keypoints_sim)) * 100 if matched_keypoints > 0 else 0
                        logger.debug(f"     - 매칭률: {match_ratio:.1f}%")

                        # Homography 산출 여부 판단
                        has_homography = matched_keypoints >= self.min_matches

                        if has_homography:
                            # Homography 행렬 산출 (가상)
                            h_matrix = [
                                [1.0, 0.0, 0.5],
                                [0.0, 1.0, 0.5],
                                [0.0, 0.0, 1.0]
                            ]
                            h_file = f"homography_{seg_idx}_frame_{frame_idx:03d}.npy"
                            h_path = os.path.join(homography_output_dir, h_file)

                            logger.debug(f"     - Homography 산출 완료")
                            logger.debug(f"       행렬: 3x3")
                            logger.debug(f"       저장: {h_path}")

                            frame_matches.append({
                                'sim_frame_index': frame_idx,
                                'sim_frame_path': sim_frame_path,
                                'matched_keypoints': matched_keypoints,
                                'match_ratio': match_ratio,
                                'has_homography': True,
                                'homography_matrix': h_matrix,
                                'homography_path': h_path
                            })
                        else:
                            logger.debug(f"     - Homography 산출 불가 (특징점 부족)")

                            frame_matches.append({
                                'sim_frame_index': frame_idx,
                                'sim_frame_path': sim_frame_path,
                                'matched_keypoints': matched_keypoints,
                                'match_ratio': match_ratio,
                                'has_homography': False,
                                'reason': f'특징점 부족 ({matched_keypoints} < {self.min_matches})'
                            })

                    except Exception as e:
                        logger.warning(f"     ❌ 프레임 매칭 실패: {sim_frame_name} - {str(e)}")
                        frame_matches.append({
                            'sim_frame_index': frame_idx,
                            'sim_frame_path': sim_frame_path,
                            'has_error': True,
                            'error': str(e)
                        })

                # 이 위치에서 성공한 매칭 수
                successful_matches = sum(1 for m in frame_matches if m.get('has_homography', False))

                matching_results.append({
                    'segmentation_index': seg_idx,
                    'frame_name': frame_name,
                    'original_frame_path': original_frame_path,
                    'inferred_address': inferred_address,
                    'original_keypoints': keypoints_original,
                    'frame_matches': frame_matches,
                    'total_sim_frames': len(generated_frames),
                    'successful_homographies': successful_matches,
                    'has_match': True
                })

                logger.debug(f"   ✓ 매칭 완료: {frame_name}")
                logger.debug(f"     - Homography 산출 성공: {successful_matches}/{len(generated_frames)}개")

            except Exception as e:
                logger.warning(f"   ❌ 매칭 실패: {frame_name} - {str(e)}")
                matching_results.append({
                    'segmentation_index': seg_idx,
                    'frame_name': frame_name,
                    'original_frame_path': original_frame_path,
                    'has_error': True,
                    'error': str(e)
                })

        logger.info(f"   ✓ 특징점 추출 및 매칭 완료: {len(matching_results)}개 위치 처리")

        # 단계 3: 결과 집계
        logger.info(f"⏳ 단계 3/4: 결과 집계 중...")
        total_homographies = sum(
            len([m for m in r.get('frame_matches', []) if m.get('has_homography', False)])
            for r in matching_results if r.get('has_match', False)
        )
        logger.info(f"   ✓ 총 Homography 산출 성공: {total_homographies}개")

        # 단계 4: 결과 저장
        logger.info(f"⏳ 단계 4/4: 결과 저장 중...")
        logger.info(f"   ✓ 매칭 결과 저장 완료: {matching_output_dir}")
        logger.info(f"   ✓ Homography 행렬 저장 완료: {homography_output_dir}")

        # 결과 반환
        result = {
            "matcher_model_name": self.matcher_model_name,
            "matching_output_dir": matching_output_dir,
            "homography_output_dir": homography_output_dir,
            "input_segmentation_count": len(selected_segmentation_pairs),
            "input_simulation_locations_count": len(location_simulation_results),
            "total_matches": len(matching_results),
            "successful_homographies": total_homographies,
            "matching_results": matching_results,
            "min_matches_threshold": self.min_matches,
        }

        return result

    def after_execute(self, context: Dict[str, Any], result: Dict[str, Any]) -> None:
        """작업 실행 후 정리 작업"""
        input_seg_count = result.get('input_segmentation_count', 0)
        input_loc_count = result.get('input_simulation_locations_count', 0)
        total_matches = result.get('total_matches', 0)
        successful_homo = result.get('successful_homographies', 0)

        logger.info(f"✅ 특징점 매칭 및 Homography 산출 작업 완료")
        logger.info(f"   📊 입력 원본 영상: {input_seg_count}개")
        logger.info(f"   📊 입력 시뮬레이션 위치: {input_loc_count}개")
        logger.info(f"   📊 매칭 처리: {total_matches}개")
        logger.info(f"   📊 Homography 산출 성공: {successful_homo}개")
        logger.info(f"   📁 결과 경로: {result.get('matching_output_dir')}")

    def on_error(self, context: Dict[str, Any], error: Exception) -> None:
        """에러 발생 시 처리"""
        logger.error(f"❌ 특징점 매칭 및 Homography 산출 작업 실패")
        logger.error(f"   에러 메시지: {str(error)}", exc_info=True)
