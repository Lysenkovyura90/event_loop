import asyncio

import aiohttp
from more_itertools import chunked

from models import Session, SwapiPeople, init_orm

MAX_REQUEST = 10


async def get_people(person_id, http_session):

    response = await http_session.get(f"https://swapi.py4e.com/api/people/{person_id}/")
    json_data = await response.json()
    return json_data


async def get_films(url_films: list) -> str:
    films = []
    for film in url_films:
        films.append(film)
    return ', '.join(films)


async def get_str(url_list: list) -> str:
    returned_list = []
    for url in url_list:
        returned_list.append(url)
    return ', '.join(returned_list)


async def insert(jsons_list):
    async with Session() as db_session:
        people_orm = []
        for json_item in jsons_list:
            orm_objects = SwapiPeople(
                    birth_year=(json_item['birth_year'] if 'birth_year' in json_item else None),
                    eye_color=(json_item['eye_color'] if 'eye_color' in json_item else None),
                    films=(await get_films(json_item['films']) if 'films' in json_item else None),
                    gender=(json_item['gender'] if 'films' in json_item else None),
                    hair_color=(json_item['hair_color'] if 'hair_color' in json_item else None),
                    height=(json_item['height'] if 'height' in json_item else None),
                    homeworld=(json_item['homeworld'] if 'homeworld' in json_item else None),
                    mass=(json_item['mass'] if 'mass' in json_item else None),
                    name=(json_item['name'] if 'name' in json_item else None),
                    skin_color=(json_item['skin_color'] if 'skin_color' in json_item else None),
                    species=(await get_str(json_item['species']) if 'species' in json_item else None),
                    starships=(await get_str(json_item['starships']) if 'starships' in json_item else None),
                    vehicles=(await get_str(json_item['vehicles']) if 'vehicles' in json_item else None))
            people_orm.append(orm_objects)
        db_session.add_all(people_orm)
        await db_session.commit()


async def main():
    await init_orm()
    async with aiohttp.ClientSession() as http_session:
        for people_id_chunk in chunked(range(1, 101), MAX_REQUEST):
            coros = [get_people(i, http_session) for i in people_id_chunk]
            jsons_list = await asyncio.gather(*coros)
            task = asyncio.create_task(insert(jsons_list))
    tasks_set = asyncio.all_tasks()
    current_task = asyncio.current_task()
    tasks_set.remove(current_task)
    await asyncio.gather(*tasks_set)


asyncio.run(main())
