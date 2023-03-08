import os.path
import traceback
from multiprocessing import Pool
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup, ResultSet
from requests import Response
from requests.adapters import HTTPAdapter, Retry

ALL_FILM_LIST_SITE = "https://www.criterion.com/shop/browse/list?sort=spine_number"
POSTER_BASE_URL = "https://s3.amazonaws.com/criterion-production/films"


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, timeout=5, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    def send(self, request, *, timeout=None, **kwargs):
        if timeout is None:
            timeout = self.timeout
        return super().send(request, timeout=timeout, **kwargs)


def _get_session(base_url: str):
    session = requests.Session()
    session.mount(
        base_url,
        adapter=TimeoutHTTPAdapter(
            timeout=60,
            max_retries=Retry(
                total=4,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS"],
                backoff_factor=0.2,
            ),
        ),
    )
    return session


def _get(url: str) -> Response:
    with _get_session(url) as session:
        res: Response = session.get(url)
        return res


def get_site() -> bytes:
    res = _get(ALL_FILM_LIST_SITE)
    return res.content


def get_all_posters(html_doc: bytes) -> List[Tuple[str, str]]:
    soup: BeautifulSoup = BeautifulSoup(html_doc, "html.parser")
    res: ResultSet = soup.findAll("img", {"data-product-box-art-image": ""})
    return [
        (
            item["alt"].replace(" ", "_").replace("/", "_"),
            item["src"].replace("_thumbnail", "_large"),
        )
        for item in res
    ]


def downloader(name: str, url: str):
    path = f"src/{name}.jpg"
    if os.path.isfile(path):
        print(f"{path} exists, skip")
        return
    with open(path, "wb+") as f:
        res = _get(url)
        res.raise_for_status()
        f.write(res.content)


def runner(*args):
    try:
        downloader(*args)
    except Exception as e:
        print(
            f"download poster for params: {args} failed, err: {traceback.print_exception(e)}"
        )


if __name__ == "__main__":
    with Pool(8) as pool:
        all_posters = get_all_posters(get_site())
        pool.starmap(runner, all_posters)
