from typing import Dict, Any, List
import os
from sv.task.task_base import TaskBase
from sv.utils.logger import setup_logger

logger = setup_logger(__name__)


class LocationSimulationTask(TaskBase):
    """위치 기반 시뮬레이션 작업 (로그 기반)"""

    def __init__(self, location_model_name: str = "LocationModel_v1", frames_per_location: int = 6):
        """
        Args:
            location_model_name: 위치 추론 모델 이름
            frames_per_location: 각 위치당 생성할 프레임 수
        """
        super().__init__("LocationSimulationTask")
        self.location_model_name = location_model_name
        self.frames_per_location = frames_per_location

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

            logger.info(f"🌍 위치 기반 시뮬레이션 준비 시작")
            logger.info(f"   📁 작업 폴더: {task_work_dir}")
            logger.info(f"   🤖 모델: {self.location_model_name}")
            logger.info(f"   📸 위치당 프레임 수: {self.frames_per_location}개")

        except Exception as e:
            logger.error(f"❌ before_execute 에러 발생: {str(e)}", exc_info=True)

    def _execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """실제 작업 실행"""
        # Primary task 결과에서 fire_location 추출
        primary_result = context.get('ConnectionTask', {})
        fire_location = primary_result.get('fire_location', {})
        original_longitude = fire_location.get('longitude')
        original_latitude = fire_location.get('latitude')

        if not original_longitude or not original_latitude:
            raise ValueError("Primary task 결과에서 fire_location을 찾을 수 없습니다")

        # 이전 task의 결과 가져오기
        segmentation_result = context.get('VideoSegmentationTask', {})
        selected_segmentation_pairs = segmentation_result.get('selected_segmentation_pairs', [])

        if not selected_segmentation_pairs:
            raise ValueError("VideoSegmentationTask 결과에서 selected_segmentation_pairs를 찾을 수 없습니다")

        task_work_dir = context.get('task_work_dir', '/tmp/location_simulation')
        simulator_result_dir = os.path.join(task_work_dir, "simulator_result")

        logger.info(f"▶️  위치 기반 시뮬레이션 처리 시작")
        logger.info(f"   📍 원본 위치: (Lon: {original_longitude}, Lat: {original_latitude})")
        logger.info(f"   📸 입력 선별 영상 수: {len(selected_segmentation_pairs)}개")
        logger.info(f"   📁 시뮬레이션 결과 경로: {simulator_result_dir}")

        # 단계 1: 원본 위치에서 Simulator 실행
        logger.info(f"⏳ 단계 1/4: 원본 위치 시뮬레이션 중...")
        logger.info(f"   ✓ Simulator 초기화 완료")
        logger.info(f"   ✓ 원본 위치 ({original_longitude}, {original_latitude})에서 {self.frames_per_location}개 영상 생성")

        original_location_results = []
        original_location_folder = os.path.join(simulator_result_dir, "original_location")
        for frame_idx in range(1, self.frames_per_location + 1):
            frame_name = f"original_location_frame_{frame_idx:03d}"
            rgb_name = f"{frame_name}.png"
            coord_name = f"{frame_name}_coord.tif"
            
            rgb_path = os.path.join(original_location_folder, rgb_name)
            coord_path = os.path.join(original_location_folder, coord_name)
            
            logger.debug(f"   ✓ 생성: {rgb_name}")
            logger.debug(f"   ✓ 생성: {coord_name} (좌표 영상)")
            
            original_location_results.append({
                'rgb_path': rgb_path,
                'coord_path': coord_path,
                'frame_index': frame_idx
            })

        logger.info(f"   ✓ 원본 위치 프레임 생성 완료: {len(original_location_results)}개 (RGB + Coord 쌍)")

        # 단계 2: Location Model로부터 각 영상의 address 추론
        logger.info(f"⏳ 단계 2/4: 위치 추론 중...")
        logger.info(f"   ✓ 모델 로드 완료: {self.location_model_name}")

        derived_locations = []

        for seg_idx, seg_pair in enumerate(selected_segmentation_pairs, 1):
            original_frame_path = seg_pair.get('original_frame_path')
            mask_path = seg_pair.get('mask_path')
            frame_name = os.path.basename(original_frame_path)

            try:
                logger.debug(f"   ▶️  [{seg_idx}/{len(selected_segmentation_pairs)}] 추론 중: {frame_name}")

                # Location model 추론 (가상)
                # 각 영상마다 다른 위치 추론
                inferred_address = f"Location_{seg_idx}_Address"
                derived_longitude = original_longitude + (seg_idx * 0.001)  # 약간씩 다른 위치
                derived_latitude = original_latitude + (seg_idx * 0.001)

                logger.debug(f"   ✓ 위치 추론 완료: {frame_name}")
                logger.debug(f"     - Address: {inferred_address}")
                logger.debug(f"     - 중심점: (Lon: {derived_longitude:.6f}, Lat: {derived_latitude:.6f})")

                derived_locations.append({
                    'segmentation_index': seg_idx,
                    'frame_name': frame_name,
                    'original_frame_path': original_frame_path,
                    'mask_path': mask_path,
                    'inferred_address': inferred_address,
                    'derived_longitude': derived_longitude,
                    'derived_latitude': derived_latitude
                })

            except Exception as e:
                logger.warning(f"   ❌ 위치 추론 실패: {frame_name} - {str(e)}")
                derived_locations.append({
                    'segmentation_index': seg_idx,
                    'frame_name': frame_name,
                    'original_frame_path': original_frame_path,
                    'mask_path': mask_path,
                    'has_error': True,
                    'error': str(e)
                })

        logger.info(f"   ✓ 위치 추론 완료: {len(derived_locations)}개 위치 추론 성공")

        # 단계 3 & 4: 각 추론된 위치로부터 시뮬레이션 영상 생성
        logger.info(f"⏳ 단계 3/4: 각 추론 위치별 시뮬레이션 중...")
        location_simulation_results = []

        for loc_info in derived_locations:
            if loc_info.get('has_error', False):
                logger.warning(f"   ⚠️  스킵 (추론 실패): {loc_info.get('frame_name')}")
                continue

            seg_index = loc_info.get('segmentation_index')
            inferred_address = loc_info.get('inferred_address')
            derived_lon = loc_info.get('derived_longitude')
            derived_lat = loc_info.get('derived_latitude')

            try:
                logger.debug(f"   ▶️  위치 {inferred_address}에서 시뮬레이션 중...")
                logger.debug(f"       좌표: (Lon: {derived_lon:.6f}, Lat: {derived_lat:.6f})")

                location_frames = []
                location_folder = os.path.join(simulator_result_dir, f"derived_location_{seg_index}")

                for frame_idx in range(1, self.frames_per_location + 1):
                    frame_name = f"derived_location_{seg_index}_frame_{frame_idx:03d}"
                    rgb_name = f"{frame_name}.png"
                    coord_name = f"{frame_name}_coord.tif"
                    
                    rgb_path = os.path.join(location_folder, rgb_name)
                    coord_path = os.path.join(location_folder, coord_name)

                    logger.debug(f"     ✓ 생성: {rgb_name}")
                    logger.debug(f"     ✓ 생성: {coord_name} (좌표 영상)")
                    
                    location_frames.append({
                        'rgb_path': rgb_path,
                        'coord_path': coord_path,
                        'frame_index': frame_idx,
                        'coordinates': {
                            'longitude': derived_lon,
                            'latitude': derived_lat
                        }
                    })

                location_simulation_results.append({
                    'segmentation_index': seg_index,
                    'inferred_address': inferred_address,
                    'coordinates': {
                        'longitude': derived_lon,
                        'latitude': derived_lat
                    },
                    'generated_frames': location_frames
                })

                logger.debug(f"     ✓ 완료: {len(location_frames)}개 프레임 쌍(RGB + Coord) 생성")

            except Exception as e:
                logger.warning(f"   ❌ 시뮬레이션 실패: {inferred_address} - {str(e)}")

        logger.info(f"⏳ 단계 4/4: 결과 정리 중...")
        logger.info(f"   ✓ {len(location_simulation_results)}개 위치별 시뮬레이션 완료")
        logger.info(f"   ✓ 원본 위치: {len(original_location_results)}개 프레임 쌍(RGB + Coord)")
        logger.info(f"   ✓ 추론 위치: {len(location_simulation_results)}개 위치")

        total_frames_generated = len(original_location_results) + sum(
            len(loc['generated_frames']) for loc in location_simulation_results
        )
        logger.info(f"   ✓ 총 생성 프레임 쌍: {total_frames_generated}개 (각각 RGB + Coord)")

        # 결과 반환
        result = {
            "location_model_name": self.location_model_name,
            "original_location": {
                'longitude': original_longitude,
                'latitude': original_latitude,
                'generated_frames': original_location_results
            },
            "simulator_result_dir": simulator_result_dir,
            "input_segmentation_count": len(selected_segmentation_pairs),
            "derived_locations": derived_locations,
            "derived_locations_count": len(location_simulation_results),
            "total_frames_generated": total_frames_generated,
            "location_simulation_results": location_simulation_results,
            "frames_per_location": self.frames_per_location,
        }

        return result

    def after_execute(self, context: Dict[str, Any], result: Dict[str, Any]) -> None:
        """작업 실행 후 정리 작업"""
        original_lon = result.get('original_location', {}).get('longitude')
        original_lat = result.get('original_location', {}).get('latitude')
        input_seg_count = result.get('input_segmentation_count', 0)
        derived_count = result.get('derived_locations_count', 0)
        total_frames = result.get('total_frames_generated', 0)

        logger.info(f"✅ 위치 기반 시뮬레이션 작업 완료")
        logger.info(f"   📍 원본 위치: (Lon: {original_lon}, Lat: {original_lat})")
        logger.info(f"   📊 입력 선별 영상: {input_seg_count}개")
        logger.info(f"   📊 추론된 위치: {derived_count}개")
        logger.info(f"   📊 생성된 프레임 쌍 (RGB + Coord): {total_frames}개")
        logger.info(f"   📁 결과 경로: {result.get('simulator_result_dir')}")

    def on_error(self, context: Dict[str, Any], error: Exception) -> None:
        """에러 발생 시 처리"""
        logger.error(f"❌ 위치 기반 시뮬레이션 작업 실패")
        logger.error(f"   에러 메시지: {str(error)}", exc_info=True)
