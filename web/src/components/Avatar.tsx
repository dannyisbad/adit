import { hashColor, initials } from "../lib/utils";

interface AvatarProps {
  name: string;
  size?: number;
}

export function Avatar({ name, size = 40 }: AvatarProps) {
  const bg = hashColor(name);
  const letters = initials(name);

  return (
    <div
      className="flex items-center justify-center rounded-full text-white font-medium shrink-0"
      style={{
        width: size,
        height: size,
        fontSize: size * 0.38,
        backgroundColor: bg,
      }}
    >
      {letters}
    </div>
  );
}
