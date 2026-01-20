<?php

declare(strict_types=1);

namespace juqngood\libmongodb;

use juqngood\libmongodb\exception\MongoException;
use juqngood\libmongodb\query\MongoQuery;
use juqngood\libmongodb\thread\MongoThread;
use pocketmine\plugin\PluginBase;
use pocketmine\utils\SingletonTrait;
use SOFe\AwaitGenerator\Await;

final class libmongodb {
	use SingletonTrait;

	/** @var array<string, array{Closure, Closure}> */
	protected array $completionHandlers = [];

	/** @var array<int, MongoThread> */
	protected array $threads = [];

	public function __construct(protected readonly PluginBase $base, string $uri, int $workers, array $config) {
		self::setInstance($this);

		for ($i = 0; $i < $workers; $i++) {
			$thread = new MongoThread($uri, $config);

			$notifier = $base->getServer()->getTickSleeper()->addNotifier(function () use ($thread) : void {
				/** @var MongoQuery|null $query */
				$query = $thread->getCompleteQueries()->shift();

				if ($query === null) return;
				$error = $query->getError() !== null ? json_decode($query->getError(), true) : null;
				$exception = $error !== null ? MongoException::fromArray($error) : null;

				[$success, $failure] = $this->completionHandlers[$query->getIdentifier()] ?? [null, null];

				match (true) {
					$exception === null && $success !== null => $success($query->getResult()),
					$exception !== null && $failure !== null => $failure($exception),
					$exception !== null => $this->base->getLogger()->logException($exception),
					default => null
				};

				if (isset($this->completionHandlers[$query->getIdentifier()])) unset($this->completionHandlers[$query->getIdentifier()]);
			});


			$thread->setSleeperHandlerEntry($notifier);
			$thread->start();

			$this->threads[] = $thread;
		}
	}

	public function submit(MongoQuery $query, ?\Closure $success = null, ?\Closure $failure = null) : void {
		$identifier = [
			spl_object_hash($query),
			microtime(),
			count($this->threads),
			count($this->completionHandlers),
		];

		$query->setIdentifier(bin2hex(implode("", $identifier)));
		$this->completionHandlers[$query->getIdentifier()] = [$success, $failure];
		$this->getLeastBusyThread()->addQuery($query);
	}

	public function asyncSubmit(MongoQuery $query) : \Generator {
		$success = yield Await::RESOLVE;
		$failure = yield Await::REJECT;

		$this->submit($query, $success, $failure);

		return yield Await::ONCE;
	}

	protected function getLeastBusyThread() : MongoThread {
		$threads = $this->threads;
		usort($threads, static fn(MongoThread $a, MongoThread $b) => $a->getQueries()->count() <=> $b->getQueries()->count());
		return $threads[0];
	}
}