import asyncio
import mimetypes
import os
import uuid
from io import BytesIO
from typing import TYPE_CHECKING, Dict, List, Optional, Union

if TYPE_CHECKING:
    from botbuilder.core import TurnContext
    from botbuilder.schema import Activity

import filetype
import httpx
from botbuilder.core import (
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
)
from botbuilder.schema import (
    Activity,
    ActivityTypes,
    Attachment,
    ChannelAccount,
    ConversationAccount,
)
from chainlit.config import config
from chainlit.context import ChainlitContext, HTTPSession, context_var
from chainlit.data import get_data_layer
from chainlit.element import Element, ElementDict
from chainlit.emitter import BaseChainlitEmitter
from chainlit.logger import logger
from chainlit.message import Message, StepDict
from chainlit.telemetry import trace
from chainlit.types import Feedback
from chainlit.user import PersistedUser, User
from chainlit.user_session import user_session


class TeamsEmitter(BaseChainlitEmitter):
    def __init__(self, session: HTTPSession, turn_context: TurnContext, enabled=False):
        super().__init__(session)
        self.turn_context = turn_context
        self.enabled = enabled

    async def send_element(self, element_dict: ElementDict):
        if not self.enabled or element_dict.get("display") != "inline":
            return

        persisted_file = self.session.files.get(element_dict.get("chainlitKey") or "")
        file: Optional[Union[BytesIO, str]] = None
        mime: Optional[str] = None

        if persisted_file:
            file = str(persisted_file["path"])
            mime = element_dict.get("mime")
        elif file_url := element_dict.get("url"):
            async with httpx.AsyncClient() as client:
                response = await client.get(file_url)
                if response.status_code == 200:
                    file = BytesIO(response.content)
                    mime = filetype.guess_mime(file)

        if not file:
            return

        element_name: str = element_dict.get("name", "Untitled")

        if mime:
            file_extension = mimetypes.guess_extension(mime)
            if file_extension:
                element_name += file_extension

        attachment = Attachment(
            content_type=mime, content_url=file_url, name=element_name
        )
        await self.turn_context.send_activity(Activity(attachments=[attachment]))

    async def send_step(self, step_dict: StepDict):
        if not self.enabled:
            return

        is_chain_of_thought = bool(step_dict.get("parentId"))
        is_empty_output = not step_dict.get("output")

        if is_chain_of_thought or is_empty_output:
            return
        else:
            enable_feedback = not step_dict.get("disableFeedback") and get_data_layer()
            message = step_dict["output"]

            if enable_feedback:
                message += "\n\n👍 / 👎"

            await self.turn_context.send_activity(message)

    async def update_step(self, step_dict: StepDict):
        if not self.enabled:
            return

        await self.send_step(step_dict)


adapter_settings = BotFrameworkAdapterSettings(
    app_id=os.environ.get("TEAMS_APP_ID"),
    app_password=os.environ.get("TEAMS_APP_PASSWORD"),
)
adapter = BotFrameworkAdapter(adapter_settings)


@trace
def init_teams_context(
    session: HTTPSession,
    turn_context: TurnContext,
) -> ChainlitContext:
    emitter = TeamsEmitter(session=session, turn_context=turn_context)
    context = ChainlitContext(session=session, emitter=emitter)
    context_var.set(context)
    user_session.set("teams_turn_context", turn_context)
    return context


users_by_teams_id: Dict[str, Union[User, PersistedUser]] = {}

USER_PREFIX = "teams_"


async def get_user(teams_user: ChannelAccount):
    if teams_user.id in users_by_teams_id:
        return users_by_teams_id[teams_user.id]

    metadata = {
        "name": teams_user.name,
        "id": teams_user.id,
    }
    user = User(identifier=USER_PREFIX + str(teams_user.id), metadata=metadata)

    users_by_teams_id[teams_user.id] = user

    if data_layer := get_data_layer():
        try:
            persisted_user = await data_layer.create_user(user)
            if persisted_user:
                users_by_teams_id[teams_user.id] = persisted_user
        except Exception as e:
            logger.error(f"Error creating user: {e}")

    return users_by_teams_id[teams_user.id]


async def download_teams_file(url: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        if response.status_code == 200:
            return response.content
        else:
            return None


async def download_teams_files(session: HTTPSession, attachments: List[Attachment]):
    download_coros = [
        download_teams_file(attachment.content_url) for attachment in attachments
    ]
    file_bytes_list = await asyncio.gather(*download_coros)
    file_refs = []
    for idx, file_bytes in enumerate(file_bytes_list):
        if file_bytes:
            name = attachments[idx].name
            mime_type = attachments[idx].content_type or "application/octet-stream"
            file_ref = await session.persist_file(
                name=name, mime=mime_type, content=file_bytes
            )
            file_refs.append(file_ref)

    files_dicts = [
        session.files[file["id"]] for file in file_refs if file["id"] in session.files
    ]

    file_elements = [Element.from_dict(file_dict) for file_dict in files_dicts]

    return file_elements


def clean_content(activity: Activity):
    return activity.text.strip()


async def process_teams_message(
    turn_context: TurnContext,
    thread_name: str,
    bind_thread_to_user=False,
):
    user = await get_user(turn_context.activity.from_property)

    thread_id = str(
        uuid.uuid5(uuid.NAMESPACE_DNS, str(turn_context.activity.conversation.id))
    )

    text = clean_content(turn_context.activity)
    teams_files = turn_context.activity.attachments

    session_id = str(uuid.uuid4())
    session = HTTPSession(
        id=session_id,
        thread_id=thread_id,
        user=user,
        client_type="teams",
    )

    ctx = init_teams_context(
        session=session,
        turn_context=turn_context,
    )

    file_elements = await download_teams_files(session, teams_files)

    msg = Message(
        content=text,
        elements=file_elements,
        type="user_message",
        author=user.metadata.get("name"),
    )

    await msg.send()

    ctx.emitter.enabled = True

    if on_chat_start := config.code.on_chat_start:
        await on_chat_start()

    if on_message := config.code.on_message:
        await on_message(msg)

    if on_chat_end := config.code.on_chat_end:
        await on_chat_end()

    if data_layer := get_data_layer():
        user_id = None
        if isinstance(user, PersistedUser):
            user_id = user.id if bind_thread_to_user else None

        try:
            await data_layer.update_thread(
                thread_id=thread_id,
                name=thread_name,
                metadata=ctx.session.to_persistable(),
                user_id=user_id,
            )
        except Exception as e:
            logger.error(f"Error updating thread: {e}")

    ctx.session.delete()


async def handle_message(turn_context: TurnContext):
    if turn_context.activity.type == ActivityTypes.message:
        thread_name = f"{turn_context.activity.from_property.name} Teams DM"
        await process_teams_message(turn_context, thread_name)


async def handle_reactions(turn_context: TurnContext):
    if turn_context.activity.type == ActivityTypes.message_reaction:
        for reaction in turn_context.activity.reactions_added:
            if reaction.type == "like":
                step_id = turn_context.activity.reply_to_id
                if data_layer := get_data_layer():
                    await data_layer.upsert_feedback(Feedback(forId=step_id, value=1))
            elif reaction.type == "dislike":
                step_id = turn_context.activity.reply_to_id
                if data_layer := get_data_layer():
                    await data_layer.upsert_feedback(Feedback(forId=step_id, value=0))


async def on_turn(turn_context: TurnContext):
    await handle_message(turn_context)
    await handle_reactions(turn_context)


# Create the main bot class
class TeamsBot:
    async def on_turn(self, turn_context: TurnContext):
        await on_turn(turn_context)


# Create the bot instance
bot = TeamsBot()
