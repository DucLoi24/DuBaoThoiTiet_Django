from rest_framework import serializers
from .models import ExtremeEvent

class ExtremeEventSerializer(serializers.ModelSerializer):
    class Meta:
        model = ExtremeEvent
        # Chọn các trường bạn muốn hiển thị trên app
        fields = [
            'event_id',
            'analysis_time',
            'severity',
            'impact_field',
            'forecast_details_vi',
            'actionable_advice_vi',
            # Bạn có thể bỏ 'location' vì API sẽ lọc theo location rồi
        ]
        read_only_fields = fields # Đảm bảo API chỉ đọc, không ghi