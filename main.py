import asyncio
import json
import shutil
import numpy as np
import requests
import aiohttp
import time
import string
import random
import pandas as pd
from typing import List, Dict


class RequestController:
    """Controller, which allows to send queue of requests"""

    def __init__(self, url: str, req_weights: Dict[str, float]):
        self.__api_url: str = url
        self.__weights: Dict[str, float] = req_weights
        self.__user_credentials: Dict[str, str] = {"email": "test@test.test",
                                                   "password": "testPassword2020"}
        self.__test_alias: Dict[str, str] = {"url": "https://github.com/",
                                             "alias": "git"}
        self.__alias: str = "git"
        self.__token: str = ""
        self.__results: List[float] = []
        self.__setup()

    @staticmethod
    def choose_scenario(req_weight: dict) -> str:
        """
        Randomly chooses request with designed probability
        :param req_weight: dictionary with request type as keys and probability as values
        :return: chosen request type
        """
        scenarios: List[str] = []
        probabilities: List[float] = []
        for scenario in req_weight.keys():
            scenarios.append(scenario)
            probabilities.append(req_weight[scenario])
        return np.random.choice(scenarios, p=probabilities)

    def __setup(self) -> None:
        """
        Basic setup: create user, add one alias to DataBase
        :return: None
        """
        requests.post(f"{self.__api_url}/users/signup/",
                      json=self.__user_credentials)
        login_response: requests.Response = requests.post(f"{self.__api_url}/users/signin/",
                                                          headers={"Content-Type": "application/json"},
                                                          data=json.dumps(self.__user_credentials))
        self.__token = login_response.json()["token"]
        requests.post(f"{self.__api_url}/urls/shorten/",
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.__token}"},
                      data=json.dumps(self.__test_alias))

    async def __request(self, req_type: str, session: aiohttp.ClientSession) -> None:
        """
        Makes request to API-server
        :param req_type: type of request
        :param session: aiohttp.ClientSession()
        :return: None
        """
        start: float = time.perf_counter()

        if req_type == "SignIn":
            resp = await session.post(f"{self.__api_url}/users/signin/",
                                      data=json.dumps(self.__user_credentials))
            await resp.text()
            duration: float = time.perf_counter() - start
            self.__results.append(duration)
        elif req_type == "SignUp":
            rand_credentials: Dict[str, str] = {
                "email": f"{''.join(random.choice(string.ascii_lowercase) for _ in range(10))}@rand.rand",
                "password": "RandPassword12"
            }
            resp = await session.post(f"{self.__api_url}/users/signup/",
                                      data=json.dumps(rand_credentials))
            await resp.text()
            duration: float = time.perf_counter() - start
            self.__results.append(duration)
        elif req_type == "Redirect":
            resp = await session.post(f"{self.__api_url}/r/{self.__alias}")
            await resp.text()
            duration: float = time.perf_counter() - start
            self.__results.append(duration)
        elif req_type == "ShortenUrl":
            rand_shorten: Dict[str, str] = {
                "url": ''.join(random.choice(string.ascii_lowercase) for _ in range(10)),
                "alias": ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
            }
            resp = await session.post(f"{self.__api_url}/urls/shorten/",
                                      headers={"Content-Type": "application/json",
                                               "Authorization": f"Bearer {self.__token}"},
                                      data=json.dumps(rand_shorten))
            await resp.text()
            duration: float = time.perf_counter() - start
            self.__results.append(duration)

    async def __make_requests(self, qps: int, requests_num: int = 30) -> None:
        """
        Makes requests to API-server with particular QPS
        :param qps: queries per second
        :param requests_num: number of requests
        :return: list responses time
        """
        pause: float = 1.0 / qps
        num_of_requests: int = requests_num

        async def gen(num_iter):
            for _ in range(num_iter):
                yield

        async with aiohttp.ClientSession() as session:
            async for i in gen(num_of_requests):
                await asyncio.ensure_future(self.__request(self.choose_scenario(self.__weights), session))
                time.sleep(pause)

    async def test_system(self, qps: int, requests_num: int = 500) -> List[float]:
        """
        Run tests by sending queries with increasing QPS rate
        :param qps: QPS
        :return: TODO print -> dataframe
        """
        await self.__make_requests(qps, requests_num)
        res: List[float] = self.__results.copy()
        self.__results = []
        # self.clean_up(r"C:\edu\load_test\load_test_server\url-shrtnr-guitarosexuals\data")
        return res

    @staticmethod
    def clean_up(path):
        """
        Clean up database
        :param path:
        :return: None
        """
        shutil.rmtree(path)


def run_multiple_tests(min_n: int, max_n: int, step: int, print_res: bool = False):
    """
    Run test with particular step
    :param min_n: start with QPS
    :param max_n: end up with QPS
    :param step: step for QPS
    :param print_res: flag, which defines whether we should print results, default - False
    :return:
    """
    request_controller = RequestController(api_url, weights)
    res: Dict[int, List[float]] = {}
    for qps in range(min_n, max_n + 1, step):
        res[qps] = asyncio.run(request_controller.test_system(qps, 500))
        sorted(res[qps])
    if print_res:
        for key in res.keys():
            print(f"{key}: {sum(res[key]) / 500}")

    data: Dict = {"QPS": [], "Latency50": [], "Latency90": [], "Latency99": []}
    for key in res.keys():
        data["QPS"].append(key)
        data["Latency50"].append(res[key][int(len(res[key]) / 50)])
        data["Latency90"].append(res[key][int(len(res[key]) / 90)])
        data["Latency99"].append(res[key][int(len(res[key]) / 99)])
    df = pd.DataFrame(data)
    df.to_csv(r"C:\edu\jupyter\University\data.csv", index=False)


if __name__ == '__main__':
    api_url: str = "http://127.0.0.1:10001"
    weights: Dict[str, float] = {
        "SignUp": 0.01,
        "SignIn": 0.005,
        "Redirect": 0.975,
        "ShortenUrl": 0.01,
    }
    run_multiple_tests(1, 1001, 50)
