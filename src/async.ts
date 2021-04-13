/* eslint-disable @typescript-eslint/await-thenable */
/* eslint-disable @typescript-eslint/unbound-method */

import { IKeyValueStoreAsync } from "@konceiver/kv";
import NeDB from "nedb";
import pify from "pify";

export class StoreAsync<K, T> implements IKeyValueStoreAsync<K, T> {
	public static async new<K, T>(
		opts: Record<string, any> = {}
	): Promise<StoreAsync<K, T>> {
		const db: NeDB = new NeDB(opts);
		await db.loadDatabase();

		db.count = pify(db.count);
		db.find = pify(db.find);
		db.insert = pify(db.insert);
		db.remove = pify(db.remove);

		return new StoreAsync<K, T>(db);
	}

	private constructor(private readonly store: NeDB) {}

	public async all(): Promise<Array<[K, T]>> {
		// @ts-ignore
		return (await this.store.find({})).map((row: { key: K; value: T }) => [
			row.key,
			row.value,
		]);
	}

	public async keys(): Promise<K[]> {
		// @ts-ignore
		return (await this.store.find({})).map((row: { key: K }) => row.key);
	}

	public async values(): Promise<T[]> {
		// @ts-ignore
		return (await this.store.find({})).map((row: { value: T }) => row.value);
	}

	public async get(key: K): Promise<T | undefined> {
		const rows = await this.store.find({ key });

		// @ts-ignore
		if (rows.length <= 0) {
			return undefined;
		}

		return rows[0].value;
	}

	public async getMany(keys: K[]): Promise<Array<T | undefined>> {
		return Promise.all([...keys].map(async (key: K) => this.get(key)));
	}

	public async pull(key: K): Promise<T | undefined> {
		const item: T | undefined = await this.get(key);

		await this.forget(key);

		return item;
	}

	public async pullMany(keys: K[]): Promise<Array<T | undefined>> {
		const items: Array<T | undefined> = await this.getMany(keys);

		await this.forgetMany(keys);

		return items;
	}

	// @ts-ignore
	public async put(key: K, value: T): Promise<boolean> {
		await this.store.insert({ key, value });

		return this.has(key);
	}

	public async putMany(values: Array<[K, T]>): Promise<boolean[]> {
		return Promise.all(
			values.map(async (value: [K, T]) => this.put(value[0], value[1]))
		);
	}

	public async has(key: K): Promise<boolean> {
		try {
			return (await this.get(key)) !== undefined;
		} catch (error) {
			return false;
		}
	}

	public async hasMany(keys: K[]): Promise<boolean[]> {
		return Promise.all([...keys].map(async (key: K) => this.has(key)));
	}

	public async missing(key: K): Promise<boolean> {
		return !(await this.has(key));
	}

	public async missingMany(keys: K[]): Promise<boolean[]> {
		return Promise.all([...keys].map(async (key: K) => this.missing(key)));
	}

	public async forget(key: K): Promise<boolean> {
		if (await this.missing(key)) {
			return false;
		}

		await this.store.remove({ key });

		return this.missing(key);
	}

	public async forgetMany(keys: K[]): Promise<boolean[]> {
		return Promise.all([...keys].map((key: K) => this.forget(key)));
	}

	public async flush(): Promise<boolean> {
		await this.store.remove({});

		return this.isEmpty();
	}

	public async count(): Promise<number> {
		// @ts-ignore
		return this.store.count({});
	}

	public async isEmpty(): Promise<boolean> {
		return (await this.count()) === 0;
	}

	public async isNotEmpty(): Promise<boolean> {
		return !(await this.isEmpty());
	}
}
