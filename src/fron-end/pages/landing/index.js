let scroll = document.querySelector(".landing_btn");
let color = document.querySelector(".landing");

scroll.addEventListener("click", function () {
    window.scroll(0, 1080);
    color.classList.add("color");
});
