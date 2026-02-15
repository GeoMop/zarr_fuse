import logging
from typing import List

from .context import ExecutionContext, ExecutionContextError
from .iterate import expand_iterate
from .render import apply_render_values
from .active_scrapper_config_models import (
    ActiveScrapperConfig,
    RunConfig,
)

LOG = logging.getLogger("active-scrapper")


def build_contexts_for_run(
    scrapper_config: ActiveScrapperConfig,
    run_cfg: RunConfig,
) -> List[ExecutionContext]:
    base = ExecutionContext(dict(run_cfg.set))

    base = apply_render_values(base, scrapper_config.render)

    contexts: List[ExecutionContext] = [base]

    for it in scrapper_config.iterate:
        next_contexts: List[ExecutionContext] = []
        for ctx in contexts:
            try:
                for new_ctx in expand_iterate(ctx, it, scrapper_config.data_source):
                    next_contexts.append(new_ctx)
            except Exception as e:
                raise ExecutionContextError(
                    f"Failed to expand iterator '{getattr(it, 'name', '<unknown>')}' "
                    f"for scrapper '{scrapper_config.name}': {e}"
                ) from e

        contexts = next_contexts

        if not contexts:
            LOG.warning(
                "Iterator '%s' produced 0 contexts for scrapper '%s' (run cron=%s).",
                getattr(it, "name", "<unknown>"),
                scrapper_config.name,
                run_cfg.cron,
            )
            break

    return contexts
