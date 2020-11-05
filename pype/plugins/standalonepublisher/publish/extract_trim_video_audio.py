import os
import pyblish.api
import pype.api

from pprint import pformat


class ExtractTrimVideoAudio(pype.api.Extractor):
    """Trim with ffmpeg "mov" and "wav" files."""

    label = "Extract Trim Video/Audio"
    hosts = ["standalonepublisher"]
    families = ["clip", "trimming"]

    # make sure it is enabled only if at least both families are available
    match = pyblish.api.Subset

    # presets

    def process(self, instance):
        representation = instance.data.get("representations")
        self.log.debug(f"_ representation: {representation}")

        if not representation:
            instance.data["representations"] = list()

        # get ffmpet path
        ffmpeg_path = pype.lib.get_ffmpeg_tool_path("ffmpeg")

        # get staging dir
        staging_dir = self.staging_dir(instance)
        self.log.info("Staging dir set to: `{}`".format(staging_dir))

        # Generate mov file.
        fps = instance.data["fps"]
        video_file_path = instance.data["editorialSourcePath"]
        extensions = instance.data.get("extensions", [".mov"])

        for ext in extensions:
            clip_trimed_path = os.path.join(
                staging_dir, instance.data["name"] + ext)
            # # check video file metadata
            # input_data = plib.ffprobe_streams(video_file_path)[0]
            # self.log.debug(f"__ input_data: `{input_data}`")

            start = float(instance.data["clipInH"])
            dur = float(instance.data["clipDurationH"])

            if ext in ".wav":
                # offset time as ffmpeg is having bug
                start += 0.5
                # remove "review" from families
                instance.data["families"] = [
                    fml for fml in instance.data["families"]
                    if "trimming" not in fml
                ]

            args = [
                ffmpeg_path,
                "-ss", str(start / fps),
                "-i", f"\"{video_file_path}\"",
                "-t", str(dur / fps)
            ]
            if ext in [".mov", ".mp4"]:
                args.extend([
                    "-crf", "18",
                    "-pix_fmt", "yuv420p"])
            elif ext in ".wav":
                args.extend([
                    "-vn -acodec pcm_s16le",
                    "-ar 48000 -ac 2"
                ])

            # add output path
            args.append(f"\"{clip_trimed_path}\"")

            self.log.info(f"Processing: {args}")
            ffmpeg_args = " ".join(args)
            output = pype.api.subprocess(ffmpeg_args, shell=True)
            self.log.info(output)

            repr = {
                "name": ext[1:],
                "ext": ext[1:],
                "files": os.path.basename(clip_trimed_path),
                "stagingDir": staging_dir,
                "frameStart": int(instance.data["frameStart"]),
                "frameEnd": int(instance.data["frameEnd"]),
                "frameStartFtrack": int(instance.data["frameStartH"]),
                "frameEndFtrack": int(instance.data["frameEndH"]),
                "fps": fps,
            }

            if ext[1:] in ["mov", "mp4"]:
                repr.update({
                    "thumbnail": True,
                    "tags": ["review", "ftrackreview", "delete"]})

            instance.data["representations"].append(repr)

            self.log.debug(f"Instance data: {pformat(instance.data)}")
