<?php

declare(strict_types=1);

namespace juqngood\libmongodb;

use juqngood\libmongodb\exception\MongoException;
use juqngood\libmongodb\query\MongoQuery;
use juqngood\libmongodb\thread\MongoThread;
use juqngood\libmongodb\thread\MongoWatchThread;
use pocketmine\plugin\PluginBase;
use pocketmine\utils\SingletonTrait;
use SOFe\AwaitGenerator\Await;

final class libmongodb {
	use SingletonTrait;

	/** @var array<string, array{0: ?\Closure, 1: ?\Closure}> */
	protected array $completionHandlers = [];

	/** @var array<int, MongoThread> */
	protected array $threads = [];

	public function __construct(
		protected readonly PluginBase $base,
		string $uri,
		int $workers,
		array $config
	) {
		self::setInstance($this);

		for ($i = 0; $i < $workers; $i++) {
			$thread = new MongoThread($uri, $config);

			$notifier = $base->getServer()->getTickSleeper()->addNotifier(function () use ($thread) : void {
				/** @var MongoQuery|null $query */
				while (($query = $thread->getCompleteQueries()->shift()) !== null) {
					$this->handleCompletedQuery($query);
				}
			});

			$thread->setSleeperHandlerEntry($notifier);
			$thread->start();

			$this->threads[] = $thread;
		}
	}

	public static function createWatch(
		string $uri,
		string $database,
		string $collection,
		array $options,
		array $piplines
	) : void {
		$thread = new MongoWatchThread(
			$uri,
			$database,
			$collection,
			$options,
			$piplines
		);

		$notifier = self::getInstance()->getBase()->getServer()->getTickSleeper()->addNotifier(function () use ($thread) : void {
			while (($operator = $thread->getOperators()->shift()) !== null) {
				$data = igbinary_unserialize($operator);

				var_dump($data);
			}
		});

		$thread->setNotifier($notifier);
		$thread->start();
	}

	public function getBase() : PluginBase {
		return $this->base;
	}

	public function submit(MongoQuery $query, ?\Closure $success = null, ?\Closure $failure = null) : void {
		$query->setIdentifier(bin2hex(random_bytes(16)));

		$this->completionHandlers[$query->getIdentifier()] = [$success, $failure];
		$this->getLeastBusyThread()->addQuery($query);
	}

	public function asyncSubmit(MongoQuery $query) : \Generator {
		$success = yield Await::RESOLVE;
		$failure = yield Await::REJECT;

		$this->submit($query, $success, $failure);

		return yield Await::ONCE;
	}

	public function waitAll(int $timeoutMs = 30000) : void {
		$start = microtime(true);

		while (count($this->completionHandlers) > 0) {
			$handled = false;

			foreach ($this->threads as $thread) {
				/** @var MongoQuery|null $query */
				while (($query = $thread->getCompleteQueries()->shift()) !== null) {
					$this->handleCompletedQuery($query);
					$handled = true;
				}
			}

			if (count($this->completionHandlers) === 0) break;

			if ((microtime(true) - $start) * 1000 >= $timeoutMs) {
				$this->base->getLogger()->warning(
					'Timed out waiting for MongoDB queries. Pending: ' . count($this->completionHandlers)
				);

				break;
			}

			if (!$handled) {
				usleep(10000);
			}
		}
	}

	private function handleCompletedQuery(MongoQuery $query) : void {
		$identifier = $query->getIdentifier();

		if (!isset($this->completionHandlers[$identifier])) return;
		[$success, $failure] = $this->completionHandlers[$identifier];
		unset($this->completionHandlers[$identifier]);

		$error = $query->getError() !== null ? json_decode($query->getError(), true) : null;
		$exception = $error !== null ? MongoException::fromArray($error) : null;

		match (true) {
			$exception === null && $success !== null => $success($query->getResult()),
			$exception !== null && $failure !== null => $failure($exception),
			$exception !== null => $this->base->getLogger()->logException($exception),
			default => null
		};
	}

	protected function getLeastBusyThread() : MongoThread {
		$threads = $this->threads;
		usort(
			$threads,
			static fn(MongoThread $a, MongoThread $b) => $a->getQueries()->count() <=> $b->getQueries()->count()
		);

		return $threads[0];
	}
}