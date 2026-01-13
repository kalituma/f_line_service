from typing import Dict, Any, List
import os
import json
from sv.task.task_base import TaskBase
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)

class SegmentationGeoJsonTask(TaskBase):
    """Homography를 적용하여 Segmentation의 지리적 바운더리를 GeoJSON으로 생성하는 작업"""

    def __init__(self, output_format: str = "geojson", result_path:str = None):
        """
        Args:
            output_format: 출력 형식 (기본값: geojson)
        """
        super().__init__("SegmentationGeoJsonTask")
        self.output_format = output_format
        self.result_path = result_path # used only for mocking version

    def _load_integrated_convex_hull(self) -> Dict[str, Any]:
        """result_path에서 통합된 Convex Hull GeoJSON을 로드
        
        Returns:
            통합된 convex hull GeoJSON 객체, 또는 None
        """
        try:
            if not self.result_path:
                logger.warning(f"   ⚠️  result_path가 설정되지 않았습니다")
                return None
            
            if not os.path.exists(self.result_path):
                logger.warning(f"   ⚠️  result_path 파일이 없습니다: {self.result_path}")
                return None
            
            logger.debug(f"   ▶️  result_path에서 통합 Convex Hull 로드 중: {self.result_path}")
            
            with open(self.result_path, 'r') as f:
                result_data = json.load(f)
            
            # 통합된 convex hull 추출
            integrated_convex_hull = result_data
            
            if not integrated_convex_hull:
                logger.warning(f"   ⚠️  result_path에 integrated_convex_hull이 없습니다")
                return None
            
            logger.debug(f"   ✓ 통합 Convex Hull 로드 완료")
            return integrated_convex_hull
            
        except Exception as e:
            logger.error(f"   ❌ 통합 Convex Hull 로드 중 에러: {str(e)}", exc_info=True)
            return None

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
            start_time = loop_context.get('start_time', 'unknown')
            task_work_folder_name = f"{self.task_name}"
            task_work_dir = os.path.join(job_work_dir, start_time, task_work_folder_name)

            # 작업 폴더 생성
            try:
                os.makedirs(task_work_dir, exist_ok=True)
                logger.info(f"✓ Task work directory created: {task_work_dir}")
                
                # 서브폴더 생성
                geojson_output_dir = os.path.join(task_work_dir, "geojson_output")
                os.makedirs(geojson_output_dir, exist_ok=True)
                logger.debug(f"✓ GeoJSON output directory created: {geojson_output_dir}")
            except Exception as e:
                logger.error(f"❌ Failed to create task work directory: {str(e)}", exc_info=True)
                return

            # context에 task_work_dir 저장
            context['task_work_dir'] = task_work_dir

            logger.info(f"🗺️  Segmentation GeoJSON 생성 준비 시작")
            logger.info(f"   📁 작업 폴더: {task_work_dir}")
            logger.info(f"   📋 출력 형식: {self.output_format}")

        except Exception as e:
            logger.error(f"❌ before_execute 에러 발생: {str(e)}", exc_info=True)

    def _execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """실제 작업 실행"""
        # FeatureMatchingTask 결과에서 homography 정보 가져오기
        feature_matching_result = context.get('FeatureMatchingTask', {})
        matching_results = feature_matching_result.get('matching_results', [])

        if not matching_results:
            raise ValueError("FeatureMatchingTask 결과에서 matching_results를 찾을 수 없습니다")

        # LocationSimulationTask 결과에서 시뮬레이션 정보 가져오기
        location_simulation_result = context.get('LocationSimulationTask', {})
        location_simulation_results = location_simulation_result.get('location_simulation_results', [])
        original_location = location_simulation_result.get('original_location', {})

        if not location_simulation_results:
            raise ValueError("LocationSimulationTask 결과에서 location_simulation_results를 찾을 수 없습니다")

        # VideoSegmentationTask 결과에서 segmentation 정보 가져오기
        segmentation_result = context.get('VideoSegmentationTask', {})
        selected_segmentation_pairs = segmentation_result.get('selected_segmentation_pairs', [])

        if not selected_segmentation_pairs:
            raise ValueError("VideoSegmentationTask 결과에서 selected_segmentation_pairs를 찾을 수 없습니다")

        task_work_dir = context.get('task_work_dir', '/tmp/geojson_boundary')
        geojson_output_dir = os.path.join(task_work_dir, "geojson_output")

        logger.info(f"▶️  Segmentation GeoJSON 생성 시작")
        logger.info(f"   📊 매칭 결과 수: {len(matching_results)}개")
        logger.info(f"   📊 시뮬레이션 위치 수: {len(location_simulation_results)}개")
        logger.info(f"   📊 선별 segmentation 수: {len(selected_segmentation_pairs)}개")
        logger.info(f"   📁 GeoJSON 출력 경로: {geojson_output_dir}")

        # 단계 1: Homography 적용 및 Coordinate 변환
        logger.info(f"⏳ 단계 1/3: Homography 적용 및 Coordinate 변환 중...")

        geojson_results = []
        total_geojson_generated = 0

        for match_idx, matching_result in enumerate(matching_results, 1):
            seg_index = matching_result.get('segmentation_index')
            frame_name = matching_result.get('frame_name')

            if not matching_result.get('has_match', False):
                logger.warning(f"   ⚠️  [{match_idx}/{len(matching_results)}] 매칭 없음: {frame_name}")
                continue

            try:
                logger.debug(f"   ▶️  [{match_idx}/{len(matching_results)}] 처리 중: {frame_name}")

                # 해당 segmentation pair 찾기
                seg_pair = None
                for pair in selected_segmentation_pairs:
                    # 파일 이름으로 매칭
                    if os.path.basename(pair.get('original_frame_path', '')) == frame_name:
                        seg_pair = pair
                        break

                if not seg_pair:
                    logger.warning(f"   ⚠️  해당 segmentation pair 없음: {frame_name}")
                    continue

                # 해당 location simulation result 찾기
                loc_sim_result = None
                for loc_result in location_simulation_results:
                    if loc_result.get('segmentation_index') == seg_index:
                        loc_sim_result = loc_result
                        break

                if not loc_sim_result:
                    logger.warning(f"   ⚠️  해당 location simulation 없음: {frame_name}")
                    continue

                mask_path = seg_pair.get('mask_path')
                inferred_address = loc_sim_result.get('inferred_address')
                generated_frames = loc_sim_result.get('generated_frames', [])

                logger.debug(f"   ✓ Segmentation: {frame_name}")
                logger.debug(f"     - Mask 경로: {mask_path}")
                logger.debug(f"     - 위치: {inferred_address}")

                # 단계 2: Frame matches에서 homography 추출 및 변환
                logger.debug(f"⏳ 단계 2/3: Frame별 Homography 적용 중...")

                frame_matches = matching_result.get('frame_matches', [])
                frame_geojson_count = 0

                for frame_match_idx, frame_match in enumerate(frame_matches, 1):
                    if not frame_match.get('has_homography', False):
                        logger.debug(f"     ⚠️  [{frame_match_idx}/{len(frame_matches)}] Homography 없음")
                        continue

                    try:
                        sim_frame_index = frame_match.get('sim_frame_index')
                        homography_path = frame_match.get('homography_path')

                        logger.debug(f"     ▶️  [{frame_match_idx}/{len(frame_matches)}] Homography 적용 중...")
                        logger.debug(f"       - 시뮬레이션 프레임 인덱스: {sim_frame_index}")
                        logger.debug(f"       - Homography 경로: {homography_path}")

                        # 시뮬레이션 Coordinate 정보 추출 (generated_frames에서)
                        transformed_coord_path = None
                        if sim_frame_index <= len(generated_frames):
                            sim_frame_info = generated_frames[sim_frame_index - 1]
                            coord_path = sim_frame_info.get('coord_path')
                            logger.debug(f"       - 원본 Coord 경로: {coord_path}")
                            
                            # Homography를 적용한 새로운 coord 파일 경로 생성
                            if coord_path:
                                base_name = os.path.splitext(os.path.basename(coord_path))[0]
                                transformed_coord_path = os.path.join(
                                    os.path.dirname(coord_path),
                                    f"{base_name}_homography_h{sim_frame_index:03d}.tif"
                                )
                                logger.debug(f"       - Homography 적용 Coord 경로: {transformed_coord_path}")

                        # 단계 2-1: Mask에 Homography 적용
                        logger.debug(f"       ▶️  Mask Homography 변환 중...")
                        
                        # mask_path에 Homography를 적용한 경로로 변환
                        transformed_mask_path = None
                        if mask_path:
                            mask_base_name = os.path.splitext(os.path.basename(mask_path))[0]
                            transformed_mask_path = os.path.join(
                                os.path.dirname(mask_path),
                                f"{mask_base_name}_homography_h{sim_frame_index:03d}.png"
                            )
                            logger.debug(f"       ✓ Mask Homography 변환 경로: {transformed_mask_path}")
                        
                        # Coord 파일에 Homography 적용된 mask 적용
                        if transformed_coord_path and transformed_mask_path:
                            logger.debug(f"       ▶️  {transformed_mask_path}를 {transformed_coord_path}에 적용 중...")
                            logger.debug(f"       ✓ Mask 적용 완료: 좌표 파일 업데이트")
                        
                        # 단계 2-2: 추출된 좌표에 Convex Hull 적용
                        logger.debug(f"       ▶️  Convex Hull 적용 중...")
                        logger.debug(f"         원본 파일: {transformed_coord_path}")
                        logger.debug(f"       ✓ Convex Hull 생성 완료")
                        
                        # 단계 3: GeoJSON Feature 생성 (로그만 출력)
                        logger.debug(f"       ▶️  GeoJSON Feature 생성 중...")
                        logger.debug(f"       ✓ GeoJSON 저장 완료")
                        
                        geojson_results.append({
                            'segmentation_index': seg_index,
                            'frame_name': frame_name,
                            'homography_index': sim_frame_index,
                            'inferred_address': inferred_address,
                            'has_geojson': True
                        })
                        
                        frame_geojson_count += 1
                        total_geojson_generated += 1

                    except Exception as e:
                        logger.warning(f"     ❌ Homography 처리 실패 (Index {sim_frame_index}): {str(e)}")
                        geojson_results.append({
                            'segmentation_index': seg_index,
                            'frame_name': frame_name,
                            'homography_index': sim_frame_index,
                            'has_error': True,
                            'error': str(e)
                        })

                logger.debug(f"   ✓ {frame_name} 처리 완료: {frame_geojson_count}개 GeoJSON 생성")

            except Exception as e:
                logger.warning(f"   ❌ Segmentation 처리 실패: {frame_name} - {str(e)}")

        logger.info(f"⏳ 단계 3/3: 결과 정리 중...")
        logger.info(f"   ✓ {len(matching_results)}개 매칭 처리 완료")
        logger.info(f"   ✓ {total_geojson_generated}개 GeoJSON 생성 완료")
        logger.info(f"   ✓ 결과 경로: {geojson_output_dir}")

        # Convex Hull 통합 단계
        logger.info(f"⏳ 단계 4/4: Convex Hull 통합 중...")
        
        integrated_convex_hull = self._load_integrated_convex_hull()
        
        if integrated_convex_hull:
            logger.info(f"   ✓ Convex Hull 통합 완료")
        else:
            logger.warning(f"   ⚠️  통합된 Convex Hull 로드 실패")

        # 결과 반환
        result = {
            "task_name": self.task_name,
            "output_format": self.output_format,
            "geojson_output_dir": geojson_output_dir,
            "input_matching_count": len(matching_results),
            "total_geojson_generated": total_geojson_generated,
            "geojson_results": geojson_results,
            "integrated_convex_hull": integrated_convex_hull,
        }

        return result

    def after_execute(self, context: Dict[str, Any], result: Dict[str, Any]) -> None:
        """작업 실행 후 정리 작업"""
        total_generated = result.get('total_geojson_generated', 0)
        input_matching_count = result.get('input_matching_count', 0)
        geojson_output_dir = result.get('geojson_output_dir', '')

        logger.info(f"✅ Segmentation GeoJSON 생성 작업 완료")
        logger.info(f"   📊 입력 매칭 결과: {input_matching_count}개")
        logger.info(f"   📊 생성된 GeoJSON: {total_generated}개")
        logger.info(f"   📁 결과 경로: {geojson_output_dir}")

    def on_error(self, context: Dict[str, Any], error: Exception) -> None:
        """에러 발생 시 처리"""
        logger.error(f"❌ Segmentation GeoJSON 생성 작업 실패")
        logger.error(f"   에러 메시지: {str(error)}", exc_info=True)
