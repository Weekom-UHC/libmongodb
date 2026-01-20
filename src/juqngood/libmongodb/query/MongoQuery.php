<?php

declare(strict_types=1);

namespace juqngood\libmongodb\query;

use Closure;
use juqngood\libmongodb\exception\MongoException;
use juqngood\libmongodb\thread\MongoThread;
use MongoDB\Client;
use pmmp\thread\ThreadSafe;
use pocketmine\thread\Thread;
use Throwable;

abstract class MongoQuery extends ThreadSafe {

	protected string $identifier = '';

	protected ?string $error = null;

	protected mixed $result = null;
	protected bool $resultSerialized = false;

	abstract public function onRun(Client $client) : void;

	public function getIdentifier() : string {
		return $this->identifier;
	}

	public function setIdentifier(string $identifier) : void {
		$this->identifier = $identifier;
	}

	public function run() : void {
		try {
			$this->onRun($this->getThread()->getConnection());
		} catch (Throwable $throwable) {
			$this->error = json_encode([
				'message' => $throwable->getMessage(),
				'code' => $throwable->getCode(),
				'trace' => $throwable->getTrace(),
				'trace_string' => $throwable->getTraceAsString(),
				'file' => $throwable->getFile(),
				'line' => $throwable->getLine(),
				'class' => $throwable instanceof MongoException ? $throwable::class : null
			]);
		}
	}

	final public function getResult() : mixed {
		return $this->resultSerialized ? igbinary_unserialize($this->result) : $this->result;
	}

	final protected function setResult(mixed $result) : void {
		$this->resultSerialized = !is_scalar($result) && !$result instanceof ThreadSafe;
		$this->result = $this->resultSerialized ? igbinary_serialize($result) : $result;
	}

	final public function getError() : ?string {
		return $this->error;
	}

	public function getThread() : MongoThread{
		$worker = Thread::getCurrentThread();
		assert($worker instanceof MongoThread);

		return $worker;
	}
}