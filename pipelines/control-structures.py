import kfp
from kfp import dsl

@dsl.component
def get_random_int_op(minimum: int, maximum: int) -> int:
    """Generate a random number between minimum and maximum (inclusive)."""
    import random
    result = random.randint(minimum, maximum)
    print(result)
    return result


@dsl.component
def flip_coin_op() -> str:
    """Flip a coin and output heads or tails randomly."""
    import random
    result = random.choice(['heads', 'tails'])
    print(result)
    return result


@dsl.component
def print_op(message: str):
    """Print a message."""
    print(message)


@dsl.component
def fail_op(message: str):
    """Fails."""
    import sys
    print(message)
    sys.exit(1)

# %% [markdown]
# ## Parallel execution
# You can use the `with dsl.ParallelFor(task1.outputs) as items:` context to execute tasks in parallel

# ## Conditional execution
# You can use the `with dsl.Condition(task1.outputs["output_name"] = "value"):` context to execute parts of the pipeline conditionally

# ## Exit handlers
# You can use `with dsl.ExitHandler(exit_task):` context to execute a task when the rest of the pipeline finishes (succeeds or fails)

# %%

@dsl.pipeline(
    name='tutorial-control-flows',
    description='Shows how to use dsl.Condition(), dsl.ParallelFor, and dsl.ExitHandler().'
)
def control_flows_pipeline():
    exit_task = print_op(message='Exit handler has worked!')
    with dsl.ExitHandler(exit_task):
        fail_op(
            message="Failing the run to demonstrate that exit handler still gets executed."
        )

    flip = flip_coin_op()

    with dsl.ParallelFor(['heads', 'tails']) as expected_result:

        with dsl.Condition(flip.output == expected_result):
            random_num_head = get_random_int_op(minimum=0, maximum=9)
            with dsl.Condition(random_num_head.output > 5):
                print_op(
                    message=f'{expected_result} and {random_num_head.output} > 5!'
                )
            with dsl.Condition(random_num_head.output <= 5):
                print_op(
                    message=f'{expected_result} and {random_num_head.output} <= 5!'
                )


if __name__ == '__main__':
    # Compiling the pipeline
    kfp.compiler.Compiler().compile(control_flows_pipeline, __file__ + '.yaml')