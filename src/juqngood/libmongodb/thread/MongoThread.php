<?php

declare(strict_types=1);

namespace juqngood\libmongodb\thread;

use juqngood\libmongodb\query\MongoQuery;
use MongoDB\Client;
use pmmp\thread\ThreadSafeArray;
use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\thread\Thread;

final class MongoThread extends Thread {

	protected SleeperHandlerEntry $sleeperHandlerEntry;

	protected ThreadSafeArray $queries;
	protected ThreadSafeArray $completeQueries;

	protected bool $running = false;

	protected Client $connection;

	public function __construct(
		protected readonly string $uri,
		protected readonly array $config
	) {
		$this->queries = new ThreadSafeArray();
		$this->completeQueries = new ThreadSafeArray();

		$this->createConnection();
	}

	/**
	 * @return ThreadSafeArray<MongoQuery>
	 */
	public function getQueries() : ThreadSafeArray {
		return $this->queries;
	}

	/**
	 * @return ThreadSafeArray<MongoQuery>
	 */
	public function getCompleteQueries() : ThreadSafeArray {
		return $this->completeQueries;
	}

	public function getConnection() : Client {
		return $this->connection;
	}

	protected function createConnection() : void {
		$this->connection = new Client(
			$this->uri,
			$this->config
		);
	}

	protected function onRun() : void {
		$notifier = $this->sleeperHandlerEntry->createNotifier();
		$this->running = true;

		$this->createConnection();

		while ($this->running) {
			$this->synchronized(
				function () : void {
					if ($this->running && $this->queries->count() === 0 && $this->completeQueries->count() === 0) {
						$this->wait();
					}
				}
			);

			if ($this->completeQueries->count() !== 0) $notifier->wakeupSleeper();
			/** @var MongoQuery|null $query */
			$query = $this->queries->shift();

			if ($query === null) continue;
			$query->run();

			$this->completeQueries[] = $query;
		}
	}

	public function quit() : void {
		$this->synchronized(
			function () : void {
				$this->running = false;
				$this->notify();
			}
		);
		parent::quit();
	}

	public function setSleeperHandlerEntry(SleeperHandlerEntry $sleeperHandlerEntry) : void {
		$this->sleeperHandlerEntry = $sleeperHandlerEntry;
	}

	public function addQuery(MongoQuery $query) : void {
		$this->synchronized(
			function () use ($query) : void {
				$this->queries[] = $query;
				$this->notify();
			}
		);
	}
}