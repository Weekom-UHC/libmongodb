<?php

declare(strict_types=1);

namespace juqngood\libmongodb\thread;

use MongoDB\Client;
use pmmp\thread\ThreadSafeArray;
use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\thread\NonThreadSafeValue;
use pocketmine\thread\Thread;

final class MongoWatchThread extends Thread {

	private SleeperHandlerEntry $notifier;

	private ThreadSafeArray $operators;

	private NonThreadSafeValue $options;
	private NonThreadSafeValue $piplines;

	public function __construct(
		private readonly string $uri,
		private readonly string $database,
		private readonly string $collection,
		array $options,
		array $piplines
	) {
		$this->options = new NonThreadSafeValue($options);
		$this->piplines = new NonThreadSafeValue($piplines);
	}

	public function setNotifier(SleeperHandlerEntry $notifier) : void {
		$this->notifier = $notifier;
	}

	protected function onRun() : void {
		$client = new Client($this->uri, $this->options->deserialize());
		$notifier = $this->notifier->createNotifier();

		$collection = $client->getCollection($this->database, $this->collection);
		$watch = $collection->watch($this->piplines->deserialize());

		foreach ($watch as $operation) {
			$this->operators[] = igbinary_serialize($operation);
			$notifier->wakeupSleeper();
		}
	}

	public function getOperators() : ThreadSafeArray {
		return $this->operators;
	}
}