import Link from "next/link";

export default function Nav() {
  return (
    <nav
      style={{
        display: "flex",
        gap: 14,
        alignItems: "center",
        justifyContent: "space-between",
        marginBottom: 18,
      }}
    >
      <div style={{ display: "flex", gap: 14 }}>
        <Link href="/">Home</Link>
        <Link href="/jobs">Jobs</Link>
        <Link href="/saved">Saved</Link>
        <Link href="/applied">Applied</Link>
      </div>
      <Link href="/login">Logout</Link>
    </nav>
  );
}
