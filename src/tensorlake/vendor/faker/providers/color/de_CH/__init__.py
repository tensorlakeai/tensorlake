from collections import OrderedDict

from tensorlake.vendor.faker.typing import OrderedDictType

from ..de import Provider as BaseProvider


class Provider(BaseProvider):
    all_colors: OrderedDictType[str, str] = OrderedDict(
        (color_name.replace("ß", "ss"), color_hexcode) for color_name, color_hexcode in BaseProvider.all_colors.items()
    )
