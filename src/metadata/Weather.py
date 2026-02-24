from enum import Enum


class Season(Enum):
    SPRING_MONTHS = [3, 4, 5]

class WeatherDescription(Enum):
    #Clear sky

    def __init__(self, value, category):
        self._value_ = value
        self.category = category

    SKY_IS_CLEAR = ("sky is clear", "clear_weather")


    # Cloudy Weather
    BROKEN_CLOUDS = ("broken clouds", "cloudy_weather")
    FEW_CLOUDS = ("few clouds", "cloudy_weather")
    SCATTERED_CLOUDS = ("scattered clouds", "cloudy_weather")
    OVERCAST_CLOUDS = ("overcast clouds", "cloudy_weather")

    # Drizzle & Light Rain
    DRIZZLE = ("drizzle", "drizzle_weather")
    LIGHT_INTENSITY_DRIZZLE = ("light intensity drizzle", "drizzle_weather")
    HEAVY_INTENSITY_DRIZZLE = ("heavy intensity drizzle", "drizzle_weather")
    LIGHT_INTENSITY_DRIZZLE_RAIN = ("light intensity drizzle rain", "drizzle_weather")
    SHOWER_DRIZZLE = ("shower drizzle", "drizzle_weather")

    # Rain
    LIGHT_RAIN = ("light rain", "rainy_weather")
    MODERATE_RAIN = ("moderate rain", "rainy_weather")
    HEAVY_INTENSITY_RAIN = ("heavy intensity rain", "rainy_weather")
    VERY_HEAVY_RAIN = ("very heavy rain", "rainy_weather")
    FREEZING_RAIN = ("freezing rain", "rainy_weather")
    PROXIMITY_MODERATE_RAIN = ("proximity moderate rain", "rainy_weather")
    PROXIMITY_SHOWER_RAIN = ("proximity shower rain", "rainy_weather")
    SHOWER_RAIN = ("shower rain", "rainy_weather")
    RAGGED_SHOWER_RAIN = ("ragged shower rain", "rainy_weather")
    LIGHT_INTENSITY_SHOWER_RAIN = ("light intensity shower rain", "rainy_weather")
    HEAVY_INTENSITY_SHOWER_RAIN = ("heavy intensity shower rain", "rainy_weather")

    # wintry weather: snow & Sleet
    SNOW = ("snow", "wintry_weather")
    LIGHT_SNOW = ("light snow", "wintry_weather")
    HEAVY_SNOW = ("heavy snow", "wintry_weather")
    SHOWER_SNOW = ("shower snow", "wintry_weather")
    LIGHT_SHOWER_SNOW = ("light shower snow", "wintry_weather")
    HEAVY_SHOWER_SNOW = ("heavy shower snow", "wintry_weather")
    RAIN_AND_SNOW = ("rain and snow", "wintry_weather")
    LIGHT_RAIN_AND_SNOW = ("light rain and snow", "wintry_weather")
    SLEET = ("sleet", "wintry_weather")
    LIGHT_SHOWER_SLEET = ("light shower sleet", "wintry_weather")

    # thunderstorm_weather
    THUNDERSTORM = ("thunderstorm", "thunderstorm_weather")
    PROXIMITY_THUNDERSTORM = ("proximity thunderstorm", "thunderstorm_weather")
    RAGGED_THUNDERSTORM = ("ragged thunderstorm", "thunderstorm_weather")
    HEAVY_THUNDERSTORM = ("heavy thunderstorm", "thunderstorm_weather")
    THUNDERSTORM_WITH_RAIN = ("thunderstorm with rain", "thunderstorm_weather")
    THUNDERSTORM_WITH_LIGHT_RAIN = ("thunderstorm with light rain", "thunderstorm_weather")
    THUNDERSTORM_WITH_HEAVY_RAIN = ("thunderstorm with heavy rain", "thunderstorm_weather")
    THUNDERSTORM_WITH_DRIZZLE = ("thunderstorm with drizzle", "thunderstorm_weather")
    THUNDERSTORM_WITH_LIGHT_DRIZZLE = ("thunderstorm with light drizzle", "thunderstorm_weather")
    THUNDERSTORM_WITH_HEAVY_DRIZZLE = ("thunderstorm with heavy drizzle", "thunderstorm_weather")
    PROXIMITY_THUNDERSTORM_WITH_RAIN = ("proximity thunderstorm with rain", "thunderstorm_weather")
    PROXIMITY_THUNDERSTORM_WITH_DRIZZLE = ("proximity thunderstorm with drizzle", "thunderstorm_weather")

    # Atmosphere / Hazards
    MIST = ("mist", "hazards")
    FOG = ("fog", "hazards")
    HAZE = ("haze", "hazards")
    DUST = ("dust", "hazards")
    SAND = ("sand", "hazards")
    SMOKE = ("smoke", "hazards")
    SQUALLS = ("squalls", "hazards")
    TORNADO = ("tornado", "hazards")
    VOLCANIC_ASH = ("volcanic ash", "hazards")
    SAND_DUST_WHIRLS = ("sand/dust whirls", "hazards")
    PROXIMITY_SAND_DUST_WHIRLS = ("proximity sand/dust whirls", "hazards")

    @classmethod
    def get_cloudy_weather(cls):
        return [c.value for c in cls if "cloudy_weather" in c.category ]

    @classmethod
    def get_drizzle_weather(cls):
        return [c.value for c in cls if "drizzle_weather" in c.category ]

    @classmethod
    def get_clear_weather(cls):
        return [c.value for c in cls if "clear_weather" in c.category ]

    @classmethod
    def get_rainy_weather(cls):
        return [c.value for c in cls if "rainy_weather" in c.category ]

    @classmethod
    def get_wintry_weather(cls):
        return [c.value for c in cls if "wintry_weather" in c.category]

    @classmethod
    def get_thunderstorm_weather(cls):
        return [c.value for c in cls if "thunderstorm_weather" in c.category]

    @classmethod
    def get_hazards(cls):
        return [c.value for c in cls if "hazards" in c.category]

    @classmethod
    def list_values(cls):
        return [c for c in cls]