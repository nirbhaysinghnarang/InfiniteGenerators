import time
import threading
from threading import Thread

# from utils.custom_logging import backend_logger
from datetime import datetime, timezone
from queue import Queue, Empty




def spawn_competing_streaming_threads(
    generator_func,
    fallback_generator_func=None,
    attempts_after_which_fallback_is_active=2,
    prompt=None,
    generator_id=None,
    verbose=True,
    retry_timeout=0.90,
    fallback_retry_timeout=5,
    global_timeout=10,
    first_token_timeout=5,
):
    """
    Threads spawner: generator_spawner
    [generator_spawner] is a Thread that keeps
    on spawning generator threads until we either receive a
    first_token from any of the threads thus far spawned
    or if we receive a sigal to terminate spawning any more threads

    The latter signal is supplied when first_token_received.wait(global_timeout)
    returns False, signalling that all of our attempts to stream a response
    failed. In this case we wait for all threads to finish and raise a TimeOut error
    function which should be handled by the caller of this function.


    If the wait(.) returns true, this function returns
    immediatedly an unpackable tuple consisting of the first_token from the winning generator,
    the winning generator itself, and a list of the threads that were created in this function.

    The caller of this function should explicitly wait for these threads to finish
    using [thread.join() for thread in threads]. This marks the threads available for
    garbage collection and is good practice for avoiding any memory leaks.

    The [generator_wrapper] function here is used as a target for each
    new thread spawned by [generator_spawner]

    The [generator_wrapper] function looks at the number of generators
    that have been currently spawned and depending on this value and [attempts_after_which_fallback_is_active]
    creates generators that are either [generator_func] or [fallback_generator_func].
    """

    # 1. Non-local variables to hold results
    first_token_received = threading.Event()
    terminate_attempts = threading.Event()

    first_token = None
    token_generator = None
    min_ttft = None
    winning_index = -1

    # Keep track of all running threads so we can join them for proper gc
    threads = []

    global_time_start = time.time()

    def verbose_print(message):
        if verbose:
            print(f"{message}, {time.time()-global_time_start} seconds after start")

    def generator_wrapper(generator_index):
        start_time = time.time()
        nonlocal first_token, token_generator, min_ttft, winning_index, threads


        if generator_index < attempts_after_which_fallback_is_active:
            token_generator_local = generator_func(
                prompt=prompt,
                generator_id=generator_index,
            )

        else:
            verbose_print(f"Using fallback generator.")
            token_generator_local = fallback_generator_func(
                prompt=prompt,
                generator_id=generator_index,
            )

        # token_generator_local = generator_func(prompt=prompt, generator_id=generator_id, generator_index=generator_index)
        queue = Queue()

        def fetch_token():
            """
            Why do we do this?
            The call to next(.) is blocking and does not have any primitives for implementing a timeout
            So, we use a queue for each generator since queue.get(.) is also blocking but implements
            a timeout natively.

            So, as soon as we have a generator, we create a local Queue and
            a thread that tries calls next(.) (which, recall, blocks) and
            puts the result of the the call into the queue.


            In the primary thread of this function, while this is happening,
            we try to get the first token from the queue (with a timeout) using queue.get(.)

            If the fetch_token(.) thread is able to complete next(.) and place
            the token in the queue before the timeout occurs, we proceed with the rest
            of the code. Otherwise queue.get(.) throws an Empty exception, which
            we catch gracefully and then throw a TimeOut error which is caught
            in the get_response function.
            """
            verbose_print(f"In queue pull thread ")
            try:
                token = next(token_generator_local)
                queue.put(token)
            except StopIteration:
                queue.put(StopIteration)
            except Exception as e:
                queue.put(e)

        fetch_token_thread = Thread(target=fetch_token)
        threads.append(fetch_token_thread)
        fetch_token_thread.start()

        verbose_print(f"Spawned new generator, index {generator_index}")
        try:

            first_token = queue.get(timeout=first_token_timeout)

            # local_ttft = time.time() - start_time
            # verbose_print(
            #     f"Generator {generator_index} received first token in {local_ttft} seconds"
            # )

            global_ttft = time.time() - global_time_start
            verbose_print(
                f"Generator {generator_index} received first token in {global_ttft} seconds"
            )

            # Before uncommenting this, we need to be sure this is called from a file where a flask app is registered with a valid SQLAlchemy instance.
            # if langsmith_extra is not None:
            #     publish_thread = Thread(target=publish_stat, args=(local_ttft,))
            #     publish_thread.start()

            if not first_token_received.is_set():
                verbose_print(
                    f"Generator {generator_index} wins the race. Setting event now."
                )
                winning_index = generator_index
                # only write ttft with the winning generator

                token_generator = token_generator_local
                # min_ttft = local_ttft
                first_token_received.set()
            else:
                verbose_print(
                    f"Generator {generator_index} lost the race. Exiting now."
                )


        except StopIteration:
            verbose_print(f"Empty generator")

        except GeneratorExit:
            verbose_print(f"Generator {token_generator_local} was closed.")

        except Empty:
            verbose_print(
                f"Timeout occurred while waiting for the first token from the generator at index {generator_index}, exiting gracefully..."
            )
            return

        except KeyError as e:
            verbose_print(f"Received invalid keys in langsmith metadata")
            raise e

    def generator_spawner():
        """
        This thread keeps on spawning new threads with a constant delay
        until the flag we're waiting for is set. these flags
        are set either when the first token from any of our generators
        is received or we exhaust the [global_timeout]
        """
        nonlocal threads
        generator_count = 0
        verbose_print("Start spawning generators.")
        while not (first_token_received.is_set() or terminate_attempts.is_set()):

            generator_thread = threading.Thread(
                target=generator_wrapper, args=(generator_count,)
            )

            threads.append(generator_thread)
            generator_thread.start()
            generator_count += 1
            verbose_print(f"{generator_count} running generators")
            if generator_count < attempts_after_which_fallback_is_active:
                time.sleep(retry_timeout)  # Threads run separately
            else:
                time.sleep(fallback_retry_timeout)

    generator_spawner_thread = Thread(target=generator_spawner)
    threads.append(generator_spawner_thread)
    generator_spawner_thread.start()

    first_token_received.wait(global_timeout)

    if first_token_received.is_set():
        verbose_print(f"Our winner -> {token_generator}, at index {winning_index}")
        if token_generator:
            return first_token, token_generator, threads
    else:
        verbose_print(
            f"All threads failed to achieve success within {global_timeout} seconds"
        )
        verbose_print("Cleaning up.")
        verbose_print("Signalling spawner thread to stop")
        terminate_attempts.set()
        verbose_print(f"All threads spawned here:{threads}")
        thread_wait_start_time = time.time()
        for thread in threads:
            thread.join()
        thread_wait_time = time.time() - thread_wait_start_time
        verbose_print(
            f"All rouge threads have now been joined. {threads}.\nThis took {thread_wait_time} seconds"
        )
        verbose_print(
            f"All threads failed to achieve success within {global_timeout} seconds"
        )
        raise TimeoutError(
            f"All threads failed to achieve success within {global_timeout} seconds"
        )

def main():
    def create_delayed_generator(delay):
        """Creates a generator with specified delay"""
        def generator(prompt, generator_id=None, generator_index=0):
            time.sleep(delay)
            message = f"Delayed({delay}s) generator -> {prompt}"
            for token in message:
                yield token
        return generator

    def failing_generator(prompt, generator_id=None, generator_index=0):
        """Generator that raises an exception"""
        raise Exception("Intentional failure")
        yield "This won't be reached"

    def empty_generator(prompt, generator_id=None, generator_index=0):
        """Generator that yields nothing"""
        return
        yield "This won't be reached"

    def instant_generator(prompt, generator_id=None, generator_index=0):
        """Generator that yields immediately"""
        message = f"Instant generator -> {prompt}"
        for token in message:
            yield token

    test_cases = [
        {
            "name": "Basic Fallback Test",
            "main_gen": create_delayed_generator(5.5),
            "fallback_gen": instant_generator,
            "attempts": 2,
            "prompt": "Test1",
            "expected_success": True,
            "description": "Main generator is slow, fallback should kick in"
        },
        {
            "name": "All Generators Fail Test",
            "main_gen": failing_generator,
            "fallback_gen": failing_generator,
            "attempts": 3,
            "prompt": "Test2",
            "expected_success": False,
            "description": "Both generators fail, should raise TimeoutError"
        },
        {
            "name": "Empty Generator Test",
            "main_gen": empty_generator,
            "fallback_gen": instant_generator,
            "attempts": 2,
            "prompt": "Test3",
            "expected_success": True,
            "description": "Main generator empty, fallback should succeed"
        },
        {
            "name": "Quick Success Test",
            "main_gen": instant_generator,
            "fallback_gen": create_delayed_generator(10),
            "attempts": 1,
            "prompt": "Test4",
            "expected_success": True,
            "description": "Main generator succeeds quickly, fallback not needed"
        },
        {
            "name": "Multiple Slow Generators Test",
            "main_gen": create_delayed_generator(3),
            "fallback_gen": create_delayed_generator(3),
            "attempts": 4,
            "prompt": "Test5",
            "expected_success": False,
            "description": "All generators too slow, should timeout"
        }
    ]

    for test_case in test_cases:
        print(f"\n{'='*50}")
        print(f"Running Test: {test_case['name']}")
        print(f"Description: {test_case['description']}")
        print(f"{'='*50}")

        try:
            start_time = time.time()
            first_token, generator, threads = spawn_competing_streaming_threads(
                test_case['main_gen'],
                fallback_generator_func=test_case['fallback_gen'],
                attempts_after_which_fallback_is_active=test_case['attempts'],
                prompt=test_case['prompt'],
                verbose=True,
                global_timeout=10,  # Keeping timeout reasonable for tests
                first_token_timeout=5
            )

            if generator:
                print("\nReceived tokens:")
                print(first_token, end='')
                for token in generator:
                    print(token, end='')
                print()

            for thread in threads:
                thread.join()

            test_duration = time.time() - start_time
            print(f"\nTest completed in {test_duration:.2f} seconds")
            
       
        except TimeoutError:
            test_duration = time.time() - start_time
            print(f"\nTimeoutError occurred after {test_duration:.2f} seconds")
            
        except Exception as e:
            test_duration = time.time() - start_time
            print(f"\nUnexpected error occurred after {test_duration:.2f} seconds: {str(e)}")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
